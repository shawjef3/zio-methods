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

package me.jeffshaw.zio.methods

import zio._
import zio.stream.Take
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Dispatches the elements of chunk-granular [[Take]]s to a pool of worker fibers
 * at element granularity, without a chunk boundary barrier.
 *
 * This is the transport/dispatch split that powers `runForeachPar`: chunks are
 * moved cheaply through the queue (one box per chunk, not per element), but every
 * worker claims individual elements out of the current chunk via a shared atomic
 * cursor, so any number of workers can be busy on the same chunk. In particular a
 * single chunk of `>= n` elements keeps all `n` workers busy — the failure mode
 * that a whole-chunk-per-worker design suffers from.
 *
 * ==Protocol==
 *
 * A [[ChunkCursorDistributor.Round]] holds the current chunk, an
 * [[AtomicInteger]] cursor, a `stride`, and a `Promise` for the next round. A
 * worker reads the current round and claims a contiguous range of elements with
 * `i = cursor.getAndAdd(stride)`:
 *
 *   - `i < chunk.length`: run `f` over `[i, min(i + stride, length))`, one
 *     element after another without returning to the cursor, then loop on the
 *     same round.
 *   - `i >= chunk.length`: the round is exhausted, and exactly one worker here is
 *     elected the ''designated fetcher''. It pulls the next [[Take]] from `fetch`
 *     and publishes the resulting round via `next`. For a data round it then
 *     loops on it; for a terminal round it returns instead (reporting the cause
 *     first, if the terminal is a failure), so it never re-observes the terminal
 *     it just published.
 *   - `i >= chunk.length` and not elected: another worker is fetching; await
 *     `next`, then loop on the round it published.
 *
 * ==Stride==
 *
 * A stride above 1 amortizes the cursor's atomic operation over several
 * elements, which is what the dispatch loop's cost is dominated by once `f` is
 * cheap. It is derived per round from `length / (n * ClaimsPerWorker)`, so it
 * engages only for rounds far larger than `n` and always leaves every worker
 * several claims — the load balance a single shared cursor exists to provide is
 * preserved, and a chunk of `>= n` elements still reaches all `n` workers.
 * Below that threshold the stride is 1 and dispatch is exactly per-element.
 *
 * The stride also decides how the fetcher is elected. At stride 1 the bases are
 * consecutive, so exactly one worker sees `i == length` and that test elects it
 * with no extra atomic — the original protocol, unchanged. A larger stride makes
 * the bases skip, so none need land on `length` at all and the same test would
 * elect nobody and hang the run; those rounds elect by CAS on `fetching`
 * instead, which costs one atomic per round on rounds that are by construction
 * large.
 *
 * A terminal [[Take]] (end-of-stream or failure) yields a ''terminal round''.
 * Any worker reaching one stops, because `loop` checks `round.terminal` before
 * touching the cursor: the worker that published it loops onto it and returns,
 * and every worker awaiting the *previous* round's `next` receives it and hits
 * the same check. A terminal round's own `next` is therefore never awaited and
 * never completed; it exists only to fill the field. This makes every worker
 * converge to termination without any worker blocking on a promise that nobody
 * will complete.
 *
 * A terminal round carries no cause. The cause of a failing terminal is reported
 * to `onError` once, by the fetcher that pulled it, so a single upstream failure
 * produces a single cause however large `n` is — matching
 * [[zio.stream.ZChannel#mapOutZIOParUnordered]], where the lone pull loop plays
 * the same role.
 *
 * ==Memory visibility==
 *
 * A non-fetcher worker learns of a new round only by awaiting `next`, and the
 * fetcher completes `next` only after fully constructing the round. Promise
 * completion/await establishes a happens-before edge, so every field the fetcher
 * wrote (chunk contents, the fresh cursor, the fresh `next` promise) is visible
 * to awaiters. Within a round, `cursor` is an `AtomicInteger`, so element claims
 * are linearized: the claimed ranges partition `[0, length)`, so no index is
 * handed out twice and none is skipped.
 */
private[methods] object ChunkCursorDistributor {

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
  private final val TrampolineEvery = 512

  /**
   * One round of dispatch over a single chunk. A ''terminal'' round is a pure
   * stop signal with an empty `chunk`: it carries no cause, because the cause of
   * a failing terminal is reported once by the fetcher that pulled it, never by
   * the workers that later observe the round.
   *
   * `chunk` is a `var` so a drained round can release it. Rounds are linked
   * forward through `next` — round k's promise resolves to round k+1 — so a
   * reference to any one round transitively reaches every later round. The seed
   * round is captured by the worker closures for the whole run, so without
   * releasing, every chunk the run has ever pulled stays reachable: retention
   * grows with the length of the stream rather than being bounded by
   * `bufferSize`. Clearing the field once the round can hand out no more
   * elements keeps the small round objects chained while letting the large
   * payload go.
   */
  private final class Round[E, A](
    @volatile var chunk: Chunk[A],
    val cursor: AtomicInteger,
    val next: Promise[Nothing, Round[E, A]],
    val terminal: Boolean,
    /**
     * How many contiguous elements one claim reserves. See [[Round.strideFor]];
     * a stride of `1` reproduces the one-element-per-atomic behavior exactly.
     */
    val stride: Int,
    /**
     * Elects the round's single designated fetcher when `stride > 1`: the first
     * worker to find the cursor at or past the end wins it by CAS.
     *
     * `null` for a stride-1 round, where it is neither allocated nor read. There
     * the bases are consecutive, so exactly one worker lands on `i == length` and
     * that implicit test elects it for free. Batched claims skip bases — a
     * stride-8 claim on a 10-element round leaves the cursor at 16 — so no base
     * need ever equal `length`, and the implicit test would elect nobody and
     * deadlock the run; only there is the extra atomic worth paying, and only
     * there is the round large enough for it to disappear into the per-round
     * cost.
     */
    val fetching: AtomicBoolean
  )

  private object Round {

    /**
     * The most elements one claim may reserve.
     *
     * A stride trades load balance against atomic traffic. A worker commits to
     * `stride` elements before it can know whether it will be the round's
     * straggler, so the tail costs up to `(stride - 1) * cost(f)` of idle time
     * for the other workers to save `(stride - 1) / stride` of the cursor's
     * atomic operations. The cap bounds that tail.
     */
    private final val MaxStride = 16

    /**
     * How many claims each worker should get per round, at minimum. This is what
     * bounds the tail imbalance in units of *work* rather than elements: with
     * `c` claims apiece, a worker that draws one oversized claim is at most
     * `1 / c` of the round behind, whatever `f` costs.
     *
     * A stride sized to give each worker exactly one claim is what an earlier
     * revision did, and it measured 7-9% *slower* at `n` in the thousands with a
     * 5ms `f`: one claim per worker means the round ends when the slowest single
     * claim ends, so a 16-element claim serialized 80ms behind the others.
     * Requiring several claims apiece keeps the same amortization for a cheap
     * `f` — where the stride is capped by `MaxStride` long before this bites —
     * while restoring fine-grained balance once elements per worker is the
     * binding constraint.
     */
    private final val ClaimsPerWorker = 8

    /**
     * Elements per claim, for a round of `length` elements dispatched to `n`
     * workers.
     *
     * `length / (n * ClaimsPerWorker)` is what preserves the concurrency
     * contract. Batching is sound only while the round still offers at least one
     * claim per worker, and this asks for several: a chunk of `>= n` elements
     * still reaches all `n` workers — the "a single chunk keeps all n workers
     * busy" guarantee — because below `n * ClaimsPerWorker` elements per round
     * the quotient is 0 and `max 1` pins the stride to 1, degrading dispatch to
     * exactly the per-element cursor.
     *
     * Batching therefore engages only where it is both safe and useful: rounds
     * far larger than `n`, which is precisely the regime where per-element
     * atomic traffic on the shared cursor is the bottleneck, and where the tail
     * a stride costs is a vanishing fraction of the round.
     */
    def strideFor(length: Int, n: Int): Int =
      if (length <= 0 || n <= 0) 1
      else ((length / (n.toLong * ClaimsPerWorker)).toInt max 1) min MaxStride

    def data[E, A](chunk: Chunk[A], n: Int): Round[E, A] = {
      val stride = strideFor(chunk.length, n)
      new Round(
        chunk,
        new AtomicInteger(0),
        makePromise[E, A],
        terminal = false,
        stride,
        // Only a batched round elects by CAS; at stride 1 the implicit
        // `i == length` test does it, so the flag is never read and is left
        // unallocated. That keeps a slow-`f` run — where every round is stride 1
        // — allocating exactly what it did before batching existed.
        if (stride == 1) null else new AtomicBoolean(false)
      )
    }

    def terminal[E, A]: Round[E, A] =
      // A terminal round's `next` is never awaited: `loop` checks `terminal`
      // before touching the cursor, so a worker that loops onto a terminal round
      // stops immediately, and a worker awaiting the *previous* round's `next`
      // receives this round and then hits that same check. The promise is
      // therefore never completed and never read; it exists only to fill the
      // field.
      new Round[E, A](
        Chunk.empty,
        new AtomicInteger(0),
        makePromise[E, A],
        terminal = true,
        stride = 1,
        fetching = null
      )

    private def makePromise[E, A]: Promise[Nothing, Round[E, A]] =
      Promise.unsafe.make[Nothing, Round[E, A]](FiberId.None)(Unsafe)
  }

  /**
   * Runs the element-dispatch loop across `n` worker fibers.
   *
   *   - `fetch` pulls the next chunk-granular [[Take]] (typically a `Queue#take`
   *     or a channel pull). It is invoked by whichever worker becomes the
   *     designated fetcher, exactly once per chunk.
   *   - `f` is the per-element callback; each worker runs at most one `f` at a
   *     time, so global concurrency is bounded by `n`.
   *   - `onError` is invoked to record a cause; recording must be
   *     idempotent/accumulating. Each distinct failure is reported exactly once:
   *     a failure from `f` by the worker that ran it, and a failing terminal by
   *     the fetcher that pulled it. Workers that merely observe the resulting
   *     terminal round do not re-report it, so one upstream failure yields one
   *     cause regardless of `n`. Fail-fast interruption of in-flight `f`
   *     invocations is the caller's responsibility, via scope interruption, which
   *     matches the existing topology.
   *
   * The returned effect completes when every worker has observed a terminal
   * round.
   */
  def run[R, E <: E1, E1, A](
    n: Int,
    fetch: ZIO[R, Nothing, Take[E, A]],
    f: A => ZIO[R, E1, Any],
    onError: Cause[E1] => ZIO[R, Nothing, Unit]
  )(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed {
      // Seed round: an already-exhausted placeholder. Its length is 0, so
      // `strideFor` takes the `length <= 0` guard and the seed is a stride-1
      // round with no `fetching` flag allocated. Election is therefore the
      // implicit one: every worker's first claim lands at or past the end,
      // exactly one of them sees `i == 0 == length` and performs the initial
      // fetch, and the rest await the round it publishes.
      val seed = Round.data[E, A](Chunk.empty, n)

      // Publishes the round the fetcher just built to the workers awaiting it.
      // `Promise#done` is the public equivalent of the internal
      // `promise.unsafe.done`: it performs the identical `completeWith`, wrapped
      // in a single `ZIO.succeed`. That wrapper costs one effect node per
      // *chunk*, never per element, so it is off the hot path.
      def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] =
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
      def release(round: Round[E, A]): Unit =
        round.chunk = null.asInstanceOf[Chunk[A]]

      // Runs `f` over one claimed range `[i, until)` of `chunk`, then returns to
      // the cursor for the next claim. Elements within a claim are chained
      // directly, so a claim of `stride` elements costs one atomic operation
      // rather than `stride` of them — the point of the whole exercise.
      //
      // `chunk` is passed in rather than re-read from the round: the fetcher may
      // null the field at any time after the boundary, and this range was
      // reserved before that could happen.
      def runClaim(round: Round[E, A], chunk: Chunk[A], i: Int, until: Int, depth: Int): ZIO[R, Nothing, Unit] =
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
        // `MaxStride` bounds a single claim at 16; it does not bound this.
        // Measured before the fix: a single 200k-element chunk with a no-op `f`
        // overflows a 512KB stack, and the error escapes as a fiber defect.
        //
        // `suspendSucceed` returns control to the interpreter, which unwinds the
        // stack and resumes from the returned effect. It is cheaper than
        // `yieldNow`, which would additionally force a scheduling round-trip.
        // The counter resets on every trampoline, so this costs one extra effect
        // node per `TrampolineEvery` elements and nothing on a suspending `f`,
        // where the chain never builds up in the first place.
        else if (depth >= TrampolineEvery) ZIO.suspendSucceed(loop(round, 0))
        else {
          // Read the chunk once. A drained round's `chunk` is nulled by the
          // fetcher, and a worker can re-enter `loop` on such a round; reading
          // into a local keeps the length checks and the element read consistent
          // with each other regardless of when that happens.
          val chunk  = round.chunk
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
            // the pre-batching loop.
            if (stride == 1) f(chunk(i)).foldCauseZIO(onError, _ => loop(round, depth + 1))
            else runClaim(round, chunk, i, (i + stride) min length, depth)
          // Fetcher election. At stride 1 the bases are consecutive, so exactly
          // one worker lands on `length` and the implicit test elects it with no
          // atomic of its own — the original protocol, unchanged. Only a stride
          // above 1 skips bases, and only there is the CAS needed; those rounds
          // are by construction large enough to absorb one atomic apiece.
          //
          // `chunk ne null` keeps a released round from re-electing: a worker
          // that re-enters `loop` on one goes to the await branch instead. For a
          // batched round the CAS would also refuse it — `release` runs only in
          // this branch, after the winner has published, so `fetching` is already
          // true by the time the chunk is nulled — which makes the guard belt and
          // braces there, and the sole protection on the stride-1 path, where
          // there is no flag to fall back on.
          else if ((chunk ne null) && (if (stride == 1) i == length else round.fetching.compareAndSet(false, true)))
            // Designated fetcher.
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
          else
            // Someone else is fetching; wait for the published round.
            round.next.await.flatMap(loop(_, 0))
        }

      ZIO.foreachParDiscard(1 to n)(_ => loop(seed, 0)).withParallelism(n)
    }
}
