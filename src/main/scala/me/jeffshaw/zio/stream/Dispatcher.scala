/*
 * Copyright 2026 Jeffrey Shaw
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.jeffshaw.zio.stream

import zio._
import zio.stream.Take
import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * The dispatch loop for one run, holding the state it is parameterized by.
 *
 * Allocated once per [[ChunkCursorDistributor.run]] and shared by every worker.
 * All five fields are immutable and only read, so the sharing needs no
 * synchronization; the mutable state of a run lives in [[Round]], not here.
 */
private[stream] final class Dispatcher[R, E <: E1, E1, A](
  n: Int,
  fetch: ZIO[R, Nothing, Take[E, A]],
  f: A => ZIO[R, E1, Any],
  onError: Cause[E1] => ZIO[R, Nothing, Unit]
)(implicit trace: Trace) {

  // Seed round: an already-exhausted placeholder. Its length is 0, so
  // `strideFor` takes the `length <= 0` guard and the seed is a stride-1
  // round with no `fetching` flag allocated. Election is therefore the
  // implicit one: every worker's first claim lands at or past the end,
  // exactly one of them sees `i == 0 == length` and performs the initial
  // fetch, and the rest await the round it publishes.
  //
  // This is a single-use election point shared by all `n` workers, and it
  // carries a precondition on how they are started. See
  // `ChunkCursorDistributor.run`, which is the only thing allowed to start
  // them: changing that call has broken the whole protocol before.
  val seed: Round[E, A] = Round.data[E, A](Chunk.empty, n)

  // Publishes the round the fetcher just built to the workers awaiting it.
  // `Promise#done` is the public equivalent of the internal
  // `promise.unsafe.done`: it performs the identical `completeWith`, wrapped
  // in a single `ZIO.succeed`. That wrapper costs one effect node per
  // *chunk*, never per element, so it is off the hot path.
  private def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] =
    round.next.done(Exit.succeed(next)).unit

  // Releases a round's chunk once it can hand out no more elements. Only the
  // designated fetcher calls this, and only after it has been elected — which
  // happens only once the cursor is at or past the end — so every element has
  // already been claimed.
  //
  // Workers still working through a claim do not touch `chunk` again: `loop`
  // reads it into a local and hands that local to `runClaim`, which carries it
  // for the whole range. Nulling the field therefore cannot affect a claim
  // already in flight, however many elements are left in it. The field is
  // `@volatile`, so a worker that re-enters `loop` on this round either sees
  // the chunk (and its cursor is past the end, sending it to the await
  // branch) or sees null and is likewise past the end.
  private def release(round: Round[E, A]): Unit =
    round.chunk = null.asInstanceOf[Chunk[A]]

  // Runs `f` over one claimed range `[i, until)` of `chunk`, then returns to
  // the cursor for the next claim. Elements within a claim are chained
  // directly, so a claim of `stride` elements costs one atomic operation
  // rather than `stride` of them — the point of the whole exercise.
  //
  // `chunk` is passed in rather than re-read from the round: the fetcher may
  // null the field at any time after the boundary, and this range was
  // reserved before that could happen.
  private def runClaim(round: Round[E, A], chunk: Chunk[A], i: Int, until: Int, depth: Int): ZIO[R, Nothing, Unit] =
    f(chunk(i)).foldCauseZIO(
      onError,
      // Continue within the claim, or go back to the cursor once it is
      // exhausted. A failure ends this worker's loop exactly as in the
      // unbatched path: the rest of the claim is abandoned, which is what
      // fail-fast means here.
      _ =>
        if (i + 1 < until) runClaim(round, chunk, i + 1, until, depth + 1)
        else loop(round, depth + 1)
    )

  // Whether this worker is the round's designated fetcher.
  //
  // At stride 1 the bases are consecutive, so exactly one worker lands on
  // `length` and the implicit test elects it with no atomic of its own — the
  // original protocol, unchanged. Only a stride above 1 skips bases, and only
  // there is the CAS needed; those rounds are by construction large enough to
  // absorb one atomic apiece.
  //
  // `chunk ne null` keeps a released round from re-electing: a worker that
  // re-enters `loop` on one goes to the await branch instead. For a batched
  // round the CAS would also refuse it — `release` runs only after the winner
  // has published, so `fetching` is already true by the time the chunk is
  // nulled — which makes the guard belt and braces there, and the sole
  // protection on the stride-1 path, where there is no flag to fall back on.
  private def isFetcher(round: Round[E, A], chunk: Chunk[A], i: Int, length: Int, stride: Int): Boolean =
    (chunk ne null) && (if (stride == 1) i == length else round.fetching.compareAndSet(false, true))

  // Pulls the next `Take` and publishes the round it yields. A terminal
  // `Take` becomes a terminal round and ends this worker; a data `Take`
  // becomes the next round, which this worker then loops onto.
  private def fetchAndPublish(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    fetch.flatMap { take =>
      take.exit.foldExit(
        cause =>
          Cause.flipCauseOption(cause) match {
            case None =>
              publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round))
            case Some(c) =>
              // The fetcher is the sole reporter of a terminal cause:
              // the round it publishes carries only the stop signal.
              publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round)) *> onError(c)
          },
        chunk => {
          val nextRound = Round.data[E, A](chunk, n)
          // Publish first, then drop this round's chunk: the successor
          // is what keeps the run moving, and after it is published no
          // worker can claim from this round again. This is what stops
          // the seed round from transitively pinning the whole stream.
          publish(round, nextRound) *> ZIO.succeed(release(round)) *> loop(nextRound, 0)
        }
      )
    }

  // Someone else is fetching; wait for the published round.
  private def awaitNext(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    round.next.await.flatMap(loop(_, 0))

  // A `ZIO.whileLoop` version of this loop was implemented and reverted: it
  // cut allocation by 23-38% while costing ~30% throughput, with
  // non-overlapping error bars. Throughput is the objective and allocation
  // only a diagnostic, so the recursive loop wins. The per-iteration graph
  // rebuilding that `whileLoop` avoids is evidently cheap enough for the
  // JIT to handle — consistent with hoisting the worker closures out of the
  // loop also measuring as a no-op. Don't retry either without a benchmark.
  def loop(round: Round[E, A], depth: Int): ZIO[R, Nothing, Unit] =
    // A terminal round only signals "stop". The cause, if any, was already
    // reported once by the fetcher that pulled it, so workers arriving here
    // must not report it again.
    if (round.terminal) Exit.unit
    // Trampoline. `foldCauseZIO` on an already-completed `Exit` runs its
    // continuation *inline* rather than returning to the ZIO interpreter, so
    // when `f` does not suspend — `Exit.unit`, `ZIO.succeed`, any pure
    // computation — the whole `loop`/`runClaim` cycle is ordinary JVM
    // recursion and the stack grows with the round, not with the claim.
    // `MaxStride` bounds a single claim; it does not bound this.
    // Measured before the fix: a single 200k-element chunk with a no-op `f`
    // overflows a 512KB stack, and the error escapes as a fiber defect.
    //
    // `suspendSucceed` returns control to the interpreter, which unwinds the
    // stack and resumes from the returned effect. It is cheaper than
    // `yieldNow`, which would additionally force a scheduling round-trip.
    // The counter resets on every trampoline, so this costs one extra effect
    // node per `TrampolineEvery` elements and nothing on a suspending `f`,
    // where the chain never builds up in the first place.
    else if (depth >= Dispatcher.TrampolineEvery) ZIO.suspendSucceed(loop(round, 0))
    else {
      // Read the chunk once. A drained round's `chunk` is nulled by the
      // fetcher, and a worker can re-enter `loop` on such a round; reading
      // into a local keeps the length checks and the element read consistent
      // with each other regardless of when that happens.
      val chunk = round.chunk
      val length = if (chunk eq null) 0 else chunk.length
      val stride = round.stride
      // One claim reserves the half-open range [i, i + stride). At stride 1
      // this is `getAndIncrement`, not `getAndAdd(1)`: the former is a JIT
      // intrinsic with a dedicated code path, and stride 1 is the hot regime
      // whenever `f` is slow enough that a round offers fewer than
      // `ClaimsPerWorker` elements per worker.
      val i = if (stride == 1) round.cursor.getAndIncrement() else round.cursor.getAndAdd(stride)
      if (i < length)
        // The claimed elements are read out of the local `chunk` before `f`
        // runs, so `f` never reaches back into the round. A stride-1 claim
        // covers a single element, so it goes straight to `f` and skips
        // `runClaim`'s range bookkeeping entirely — that path is then exactly
        // the pre-batching loop, and stays inline here so it gains no frame.
        if (stride == 1) f(chunk(i)).foldCauseZIO(onError, _ => loop(round, depth + 1))
        else runClaim(round, chunk, i, (i + stride) min length, depth)
      else if (isFetcher(round, chunk, i, length, stride)) fetchAndPublish(round)
      else awaitNext(round)
    }
}

private[stream] object Dispatcher {

  /**
   * How many consecutive elements a worker may run before returning control to
   * the ZIO interpreter, unwinding the JVM stack.
   *
   * `foldCauseZIO` on an already-completed `Exit` invokes its continuation
   * inline, so a synchronous `f` — `Exit.unit`, `ZIO.succeed`, any pure
   * computation — turns the `loop`/`runClaim` cycle into plain JVM recursion
   * whose depth is the length of the round. Measured before this existed: a
   * single 200,000-element chunk with a no-op `f` overflows a 512KB stack, and
   * the `StackOverflowError` escapes as a fiber defect rather than something a
   * caller can catch. A suspending `f` never builds the chain, which is why the
   * existing tests and the I/O-shaped benchmarks never hit it.
   *
   * 512 sits ~200x below the measured overflow point on the smallest stack
   * tested, and costs one extra effect node per 512 elements — under 0.2% of
   * the per-element work even when `f` is a no-op.
   */
  private[stream] final val TrampolineEvery = 512

}
