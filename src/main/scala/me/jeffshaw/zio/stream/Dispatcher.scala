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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

/**
 * The dispatch loop for one run, holding the state it is parameterized by.
 *
 * Allocated once per [[ChunkCursorDistributor.run]] and shared by every worker.
 * Its parameters are immutable and only read, so the sharing needs no
 * synchronization. The mutable state of a run lives in [[Round]]; the only
 * mutable state here is the seed handoff, touched once per worker at startup.
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
  // fetch, and the rest await the round it publishes. Arrival order does not
  // matter: a worker that starts after the seed was released finds
  // `chunk == null`, cannot be elected, and follows `seed.next` like the rest.
  //
  // Handed out through a reference that the last worker to start clears,
  // rather than kept in a field, because rounds link forward through `next`:
  // anything that still reaches the seed reaches every round the run has
  // produced. A field would be reachable for the whole run, since every
  // worker's continuations close over this dispatcher.
  private[this] val seedRef = new AtomicReference[Round[E, A]](Round.data[E, A](Chunk.empty, n))
  private[this] val started = new AtomicInteger(0)

  // The newest round published so far. A worker that finishes `f` resumes
  // here rather than on the round it claimed from, because the continuation
  // of an `f` still running is reachable for as long as it runs: if it held
  // that round, then through `next` it would hold every round published since,
  // so one slow or hung `f` would retain the stream's rounds until it
  // returned. Holding the newest round pins nothing earlier, since the links
  // point forward.
  //
  // Jumping ahead skips nothing. A round is superseded only once its fetcher
  // found the cursor at or past the end, so every element of every older round
  // has already been claimed. Election does not depend on which round a worker
  // arrives at, as the seed shows. `publish` writes it before completing the
  // promise that hands the round out, so a worker that has claimed from a
  // round reads that round or a newer one, never null.
  @volatile private[this] var current: Round[E, A] = null

  // What every worker starts with. The seed is read inside the suspension,
  // rather than captured by an effect built up front, because this effect
  // stays reachable for the worker's whole life. `WorkerPool` wraps it in
  // `ensuring`, a `foldCauseZIO` whose frame sits at the bottom of the
  // worker's stack until it exits and holds the wrapped effect; with `n == 1`
  // there is no fork, and the `.unit` map frame holds it on the calling fiber
  // instead. An effect that captured the seed, as `loop(seed)` does once
  // called, would pin the whole chain from there.
  //
  // Each worker reads before it counts itself, so the `n`-th increment comes
  // after all `n` reads and no worker can find the reference already cleared.
  private[this] val start: ZIO[R, Nothing, Unit] = ZIO.suspendSucceed {
    val seed = seedRef.get
    if (started.incrementAndGet() == n) seedRef.set(null)
    loop(seed)
  }

  /**
   * Runs the `n` workers, completing once every one of them has observed a
   * terminal round. Call it at most once: the seed goes to the first `n`
   * workers to start.
   *
   * A worker can die even though its type says it cannot fail: `loop` calls `f`
   * directly, so an `f` that throws while building its effect throws inside a
   * continuation, and the runtime turns that into a defect that unwinds past the
   * element's `foldCauseZIO`, which was never built. `WorkerPool` only counts
   * exits, so without the handler here that death would be lost: the run would
   * carry on a worker short, and once every worker had died it would stop
   * consuming the stream and still report success. At `n == 1`, which does not
   * fork, the same defect would fail the run instead. Handling it once per
   * worker sends it through `onError` like any other failure of `f`, at no
   * per-element cost, and the fold frame it adds holds only `start`, which
   * does not reach the seed. External interruption never reaches the handler,
   * since the runtime skips fold handlers on an interrupted fiber, and
   * `onError` ignores interruption-only causes regardless.
   */
  def run: ZIO[R, Nothing, Unit] = WorkerPool.replicate(n)(start.catchAllCause(onError))

  /**
   * The seed round until every worker has started, then `null`. Nothing in a
   * run reads it; it lets `RetentionSpec` follow the round chain from its root.
   */
  private[stream] def seedForTesting: Round[E, A] = seedRef.get

  // Publishes the round the fetcher just built to the workers awaiting it.
  // `Promise#done` is the public equivalent of the internal
  // `promise.unsafe.done`: it performs the identical `completeWith`.
  //
  // It also makes `next` the round finished callbacks resume from. That write
  // happens when `publish` is called, which is before the returned effect
  // completes the promise.
  private def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] = {
    current = next
    round.next.done(Exit.succeed(next)).unit
  }

  // Releases a round's chunk once it can hand out no more elements. Only the
  // designated fetcher calls this, and only after it has been elected, which
  // happens only once the cursor is at or past the end, so every element has
  // already been claimed.
  //
  // It runs before the fetch rather than after the publish. The fetch can
  // wait on the producer for as long as the stream is idle, and for all that
  // time the drained round is reachable twice over: from the fetcher's own
  // continuation, and from `current`, which still names it. Released only
  // after the publish, its chunk, a fusion of up to `bufferSize` of the
  // stream's chunks, stayed alive until the next chunk arrived. Measured with
  // 16 chunks of 100 emitted before an idle stream: 400 to 1600 elements
  // reachable, against the 100 of the chunk the stream machinery itself keeps.
  //
  // Workers still working through a claim do not touch `chunk` again: `loop`
  // reads it into a local and hands that local, or a copy of what is left of
  // the claim, to `runClaim`, which carries it for the whole range. Nulling
  // the field therefore cannot affect a claim already in flight, however many
  // elements are left in it. The field is
  // `@volatile`, so a worker that re-enters `loop` on this round either sees
  // the chunk (and its cursor is past the end, sending it to the await
  // branch) or sees null and is likewise past the end.
  private def release(round: Round[E, A]): Unit =
    round.chunk = null.asInstanceOf[Chunk[A]]

  // What a worker runs once an `f` that did not complete synchronously
  // finishes the last element of its claim. Built once per run rather than
  // per element: it needs no per-element state, because it resumes from
  // `current` rather than from the round it claimed from (see `current`), and
  // the trampoline budget in `loop` starts afresh after any suspension.
  private[this] val resume: Any => ZIO[R, Nothing, Unit] = _ => loop(current)

  // `loop` as a function value, so awaiting a round does not allocate one.
  private[this] val loopFn: Round[E, A] => ZIO[R, Nothing, Unit] = round => loop(round)

  // Continues a claim `[from, until)` of `chunk` after an element whose `f`
  // did not complete synchronously. Elements within a claim cost no atomic
  // operation, which is the point of claiming several at once.
  //
  // `chunk` is passed in rather than re-read from the round: the fetcher may
  // null the field at any time after the boundary, and this range was
  // reserved before that could happen. The loop is bounded by the claim,
  // which `Round.MaxStride` caps, so it needs no trampoline budget of its own.
  private def runClaim(chunk: Chunk[A], from: Int, until: Int): ZIO[R, Nothing, Unit] = {
    var j = from
    while (j < until) {
      val effect = f(chunk(j))
      j += 1
      effect match {
        case _: Exit.Success[_] => ()
        // A failure ends this worker's loop: the rest of the claim is
        // abandoned, which is what fail-fast means here.
        case failure: Exit.Failure[E1 @unchecked] => return onError(failure.cause)
        case _ => return continueAfter(effect, chunk, j, until)
      }
    }
    loop(current)
  }

  // Sequences what comes after an `f` that did not complete synchronously:
  // the rest of its claim, or, at the end of the claim, back to the cursor of
  // the newest round.
  //
  // What the continuation captures matters, because the continuation of an
  // `f` that has not returned is reachable for as long as it runs. Capturing
  // `chunk` there would keep the whole round's chunk, a fusion of up to
  // `bufferSize` of the stream's chunks, alive behind every hung or slow
  // callback, long after `release` let the round go: measured with 64
  // workers, one hung callback kept 1697 elements reachable against 99 for
  // `mapZIOParUnordered`. So the continuation captures only what the claim
  // still needs:
  //
  //   - At the end of the claim, nothing: the pre-built `resume`.
  //   - Mid-claim, a copy of the rest of the claim, at most
  //     `Round.MaxStride - 1` elements. A chunk that small is kept as is, so
  //     a claim copies at most once however often its `f` suspends.
  //
  // An `f` that completed synchronously never reaches here: `loop` and
  // `runClaim` handle those results inline, so nothing captures `chunk`
  // beyond the current JVM frame.
  private def continueAfter(effect: ZIO[R, E1, Any], chunk: Chunk[A], next: Int, until: Int): ZIO[R, Nothing, Unit] =
    if (next >= until) effect.foldCauseZIO(onError, resume)
    else if (chunk.length <= Round.MaxStride) effect.foldCauseZIO(onError, _ => runClaim(chunk, next, until))
    else {
      val rest = chunk.slice(next, until).materialize
      effect.foldCauseZIO(onError, _ => runClaim(rest, 0, rest.length))
    }

  // Whether this worker is the round's designated fetcher.
  //
  // At stride 1 the bases are consecutive, so exactly one worker lands on
  // `length` and the implicit test elects it with no atomic of its own: the
  // original protocol, unchanged. Only a stride above 1 skips bases, and only
  // there is the CAS needed; those rounds are by construction large enough to
  // absorb one atomic apiece.
  //
  // `chunk ne null` keeps a released round from re-electing: a worker that
  // re-enters `loop` on one goes to the await branch instead. For a batched
  // round the CAS would also refuse it, since `release` runs only once the
  // winner has been elected, so `fetching` is already true by the time the
  // chunk is nulled. That makes the guard belt and braces there, and the sole
  // protection on the stride-1 path, where there is no flag to fall back on.
  private def isFetcher(round: Round[E, A], chunk: Chunk[A], i: Int, length: Int, stride: Int): Boolean =
    (chunk ne null) && (if (stride == 1) i == length else round.fetching.compareAndSet(false, true))

  // Releases the drained round, then pulls the next `Take` and publishes the
  // round it yields. A terminal `Take` becomes a terminal round and ends this
  // worker; a data `Take` becomes the next round, which this worker then
  // loops onto.
  private def fetchAndPublish(round: Round[E, A]): ZIO[R, Nothing, Unit] = {
    release(round)
    fetch.flatMap { take =>
      take.exit.foldExit(
        cause =>
          Cause.flipCauseOption(cause) match {
            case None =>
              publish(round, Round.terminal[E, A])
            case Some(c) =>
              // The fetcher is the sole reporter of a terminal cause:
              // the round it publishes carries only the stop signal.
              publish(round, Round.terminal[E, A]) *> onError(c)
          },
        chunk => {
          val nextRound = Round.data[E, A](chunk, n)
          publish(round, nextRound) *> loop(nextRound)
        }
      )
    }
  }

  // Someone else is fetching; wait for the published round.
  private def awaitNext(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    round.next.await.flatMap(loopFn)

  // Claims from `round` until it runs out, running `f` on each claimed element.
  //
  // An `f` that returns an already-completed `Exit` (`Exit.unit`,
  // `Exit.succeed(a)`) is handled right here, in a plain JVM loop: its result
  // is known, so there is nothing to sequence and no effect node or closure to
  // build. Every other effect, `ZIO.succeed` and `ZIO.unit` included, is handed
  // back to the interpreter with a continuation, the pre-built `resume` when
  // the claim ends with it. So the loop never recurses on the JVM stack.
  //
  // This is not the `ZIO.whileLoop` version that was implemented and reverted
  // (it cut allocation by 23-38% while costing ~30% throughput): that one ran
  // every element through the interpreter. Hoisting the per-element closure
  // alone also measured as a no-op before; what this removes for an `Exit`
  // result is the closure, the recursion and the interpreter round trip
  // together.
  def loop(round: Round[E, A]): ZIO[R, Nothing, Unit] = {
    // How many synchronously completed elements may run before control goes
    // back to the interpreter. See `Dispatcher.TrampolineEvery`.
    var budget = Dispatcher.TrampolineEvery
    while (true) {
      // A terminal round only signals "stop". The cause, if any, was already
      // reported once by the fetcher that pulled it, so workers arriving here
      // must not report it again.
      if (round.terminal) return Exit.unit
      // Read the chunk once. A drained round's `chunk` is nulled by the
      // fetcher, and a worker can re-enter `loop` on such a round; reading
      // into a local keeps the length checks and the element reads consistent
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
      if (i >= length)
        return if (isFetcher(round, chunk, i, length, stride)) fetchAndPublish(round) else awaitNext(round)
      // The claimed elements are read out of the local `chunk` before `f`
      // runs, so `f` never reaches back into the round, and a suspended `f`
      // resumes from `current` rather than capturing `round`, so an `f` that
      // runs long pins no rounds.
      val until = if (stride == 1) i + 1 else (i + stride) min length
      var j = i
      while (j < until) {
        val effect = f(chunk(j))
        j += 1
        effect match {
          case _: Exit.Success[_] => ()
          // A failure ends this worker's loop: the rest of the claim is
          // abandoned, which is what fail-fast means here.
          case failure: Exit.Failure[E1 @unchecked] => return onError(failure.cause)
          case _ => return continueAfter(effect, chunk, j, until)
        }
      }
      budget -= until - i
      if (budget <= 0) return ZIO.suspendSucceed(loop(round))
    }
    // Unreachable: every way out of the loop above is a `return`.
    Exit.unit
  }
}

private[stream] object Dispatcher {

  /**
   * How many elements whose `f` completed synchronously a worker may run in
   * `loop` before handing control back to the ZIO interpreter.
   *
   * An `f` that returns an already-completed `Exit` is handled inline, so
   * without a bound one worker could run an entire round, however large,
   * without ever returning to the interpreter. It would then never reach an
   * operation boundary, so it would not yield its thread to other fibers and
   * would not notice interruption: a fail-fast teardown would wait for the
   * round. Returning an effect every so often restores both, and costs one
   * `suspendSucceed` node per `TrampolineEvery` elements.
   *
   * This used to bound JVM recursion as well, back when an `Exit` result ran
   * its continuation inline through `Exit#foldCauseZIO`: a single
   * 200,000-element chunk with a no-op `f` overflowed a 512KB stack. `loop` no
   * longer recurses, and `StackSafetySpec` still covers that case.
   */
  private final val TrampolineEvery = 512

}
