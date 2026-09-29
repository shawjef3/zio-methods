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
 * Dispatches the elements of chunk-granular [[Take]]s to a pool of worker fibers
 * at element granularity, without a chunk boundary barrier.
 *
 * This is the transport/dispatch split that powers `runForeachPar`: chunks are
 * moved cheaply through the queue (one box per chunk, not per element), but every
 * worker claims individual elements out of the current chunk via a shared atomic
 * cursor, so any number of workers can be busy on the same chunk. In particular a
 * single chunk of `>= n` elements keeps all `n` workers busy, the failure mode
 * that a whole-chunk-per-worker design suffers from.
 *
 * ==Protocol==
 *
 * A [[Round]] holds the current chunk, an [[AtomicInteger]] cursor, a `stride`,
 * and a `Promise` for the next round. A worker reads the current round and
 * claims a contiguous range of elements with `i = cursor.getAndAdd(stride)`:
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
 * several claims, so the load balance a shared cursor exists to provide is
 * preserved, and a chunk of `>= n` elements still reaches all `n` workers.
 * Below that threshold the stride is 1 and dispatch is exactly per-element.
 *
 * The stride also decides how the fetcher is elected. At stride 1 the bases are
 * consecutive, so exactly one worker sees `i == length` and that test elects it
 * with no extra atomic. A larger stride makes the bases skip, so none need land
 * on `length` at all and the same test would elect nobody and hang the run;
 * those rounds elect by CAS on `fetching` instead, which costs one atomic per
 * round on rounds that are by construction large.
 *
 * A terminal [[Take]] (end-of-stream or failure) yields a ''terminal round''.
 * Any worker reaching one stops, because `loop` checks `round.terminal` before
 * touching the cursor: the worker that published it loops onto it and returns,
 * and every worker awaiting the *previous* round's `next` receives it and hits
 * the same check. A terminal round's own `next` and cursor are therefore never
 * read, and are left null. This makes every worker converge to termination
 * without any worker blocking on a promise that nobody will complete.
 *
 * A terminal round carries no cause. The cause of a failing terminal is reported
 * to `onError` once, by the fetcher that pulled it, so a single upstream failure
 * produces a single cause however large `n` is, matching
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
 *
 * ==This class==
 *
 * One instance per run, built by [[Dispatcher.run]] and shared by every worker.
 * Holding `n`, `fetch`, `f` and `onError` as fields keeps them off the loop's
 * argument lists: as nested defs they were lifted into every call, including
 * the per-element ones. They are immutable and only read, so the sharing needs
 * no synchronization. The mutable state of a run lives in [[Round]]; the only
 * mutable state here is the seed handoff, touched once per worker at startup,
 * and [[current]].
 */
private[stream] final class Dispatcher[R, E <: E1, E1, A](
  n: Int,
  fetch: ZIO[R, Nothing, Take[E, A]],
  f: A => ZIO[R, E1, Any],
  onError: Cause[E1] => ZIO[R, Nothing, Unit]
)(implicit trace: Trace) {

  /**
   * The seed round: an already-exhausted placeholder, so every worker's first
   * claim lands at or past its end. Its length is 0, making it a stride-1 round,
   * so exactly one worker sees `i == 0 == length` and performs the initial fetch
   * while the rest await the round it publishes. Arrival order does not matter:
   * a worker that starts after the seed was released finds `chunk == null`,
   * cannot be elected, and follows `seed.next` like the rest.
   *
   * Handed out through a reference that the last worker to start clears, rather
   * than kept in a field: anything still reaching the seed reaches every round
   * the run has produced (see [[Round]]), and every worker's continuations close
   * over this dispatcher.
   */
  private[this] val seedRef = new AtomicReference[Round[E, A]](Round.data[E, A](Chunk.empty, n))
  private[this] val started = new AtomicInteger(0)

  /**
   * The newest round published so far, which a worker resumes from after an `f`
   * that did not complete synchronously. Resuming from the round it claimed from
   * instead would mean the continuation of a still-running `f` held that round,
   * and so every round published after it, until `f` returned.
   *
   * Jumping ahead skips nothing: a round is superseded only once its fetcher
   * found the cursor at or past the end, so every element of every older round
   * has already been claimed, and election does not depend on which round a
   * worker arrives at. [[publish]] writes it before completing the promise that
   * hands the round out, so a worker that has claimed from a round reads that
   * round or a newer one, never null.
   */
  @volatile private[this] var current: Round[E, A] = null

  /**
   * What every worker starts with. The seed is read inside the suspension rather
   * than captured by an effect built up front, because this effect stays
   * reachable for the worker's whole life: [[WorkerPool]] wraps it in `ensuring`,
   * whose fold frame sits at the bottom of the worker's stack until it exits,
   * and at `n == 1` the `.unit` map frame holds it on the calling fiber instead.
   * An effect that captured the seed, as `loop(seed)` does once called, would pin
   * the whole round chain from there.
   *
   * Each worker reads the seed before it counts itself, so the `n`-th increment
   * comes after all `n` reads and no worker can find the reference cleared.
   */
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

  /**
   * Publishes the round the fetcher just built to the workers awaiting `round`,
   * and makes it [[current]]. That write happens when `publish` is called, before
   * the returned effect completes the promise. `Promise#done` is the public
   * equivalent of the internal `promise.unsafe.done`: the same `completeWith`.
   */
  private def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] = {
    current = next
    round.next.done(Exit.succeed(next)).unit
  }

  /**
   * Releases a drained round's chunk. Only the elected fetcher calls this, and
   * election happens only once the cursor is at or past the end, so every
   * element has already been claimed.
   *
   * It runs before the fetch rather than after the publish. The fetch can wait
   * on the producer for as long as the stream is idle, and all that time the
   * drained round is reachable from the fetcher's continuation and from
   * [[current]]. Released only after the publish, its chunk, a fusion of up to
   * `bufferSize` of the stream's chunks, stayed alive until the next chunk
   * arrived: 400 to 1600 elements after a 16-chunk burst of 100, against the
   * 100 of the chunk the stream machinery itself keeps.
   *
   * A claim in flight never re-reads the field: `loop` reads it into a local
   * and hands that, or a copy of what is left of the claim, to `runClaim`. The
   * field is `@volatile`, so a worker that re-enters `loop` on this round sees
   * either the chunk with its cursor past the end, or null; both send it to the
   * await branch.
   */
  private def release(round: Round[E, A]): Unit =
    round.chunk = null.asInstanceOf[Chunk[A]]

  /**
   * The continuation after an `f` that did not complete synchronously and ended
   * its claim. Built once per run: it needs no per-element state, since it
   * resumes from [[current]] and `loop`'s trampoline budget starts afresh after
   * any suspension.
   */
  private[this] val resume: Any => ZIO[R, Nothing, Unit] = _ => loop(current)

  /** `loop` as a function value, so awaiting a round does not allocate one. */
  private[this] val loopFn: Round[E, A] => ZIO[R, Nothing, Unit] = round => loop(round)

  /**
   * Continues a claim `[from, until)` of `chunk` after an element whose `f` did
   * not complete synchronously, handling each result as [[loop]] does. Elements
   * within a claim cost no atomic operation, which is the point of claiming
   * several at once.
   *
   * `chunk` is passed in rather than re-read from the round, which the fetcher
   * may release at any time after the boundary; this range was reserved before
   * that could happen. The loop is bounded by the claim, which
   * `Round.MaxStride` caps, so it needs no trampoline budget of its own.
   */
  private def runClaim(chunk: Chunk[A], from: Int, until: Int): ZIO[R, Nothing, Unit] = {
    var j = from
    while (j < until) {
      val effect = f(chunk(j))
      j += 1
      effect match {
        case _: Exit.Success[_] => ()
        case failure: Exit.Failure[E1 @unchecked] => return onError(failure.cause)
        case _ => return continueAfter(effect, chunk, j, until)
      }
    }
    loop(current)
  }

  /**
   * Sequences what comes after an `f` that did not complete synchronously: the
   * rest of its claim, or, at the end of the claim, [[resume]].
   *
   * What the continuation captures matters, because the continuation of an `f`
   * that has not returned is reachable for as long as it runs. Capturing `chunk`
   * would keep the whole round's chunk, a fusion of up to `bufferSize` of the
   * stream's chunks, alive behind every hung or slow callback, long after
   * [[release]]: with 64 workers, one hung callback kept 1697 elements reachable
   * against 99 for `mapZIOParUnordered`. So mid-claim it captures a copy of the
   * rest of the claim, at most `Round.MaxStride - 1` elements. A chunk that
   * small is kept as is, so a claim copies at most once however often its `f`
   * suspends.
   */
  private def continueAfter(effect: ZIO[R, E1, Any], chunk: Chunk[A], next: Int, until: Int): ZIO[R, Nothing, Unit] =
    if (next >= until) effect.foldCauseZIO(onError, resume)
    else if (chunk.length <= Round.MaxStride) effect.foldCauseZIO(onError, _ => runClaim(chunk, next, until))
    else {
      val rest = chunk.slice(next, until).materialize
      effect.foldCauseZIO(onError, _ => runClaim(rest, 0, rest.length))
    }

  /**
   * Whether this worker is the round's elected fetcher: by `i == length` at
   * stride 1, and by CAS on `fetching` for a batched round (see
   * the class doc on why the stride decides).
   *
   * `chunk ne null` keeps a released round from re-electing: a worker that
   * re-enters `loop` on one goes to the await branch instead. For a batched
   * round the CAS would refuse it too, since `release` runs only once the winner
   * has been elected, so the guard is belt and braces there and the sole
   * protection at stride 1.
   */
  private def isFetcher(round: Round[E, A], chunk: Chunk[A], i: Int, length: Int, stride: Int): Boolean =
    (chunk ne null) && (if (stride == 1) i == length else round.fetching.compareAndSet(false, true))

  /**
   * Releases the drained round, then pulls the next `Take` and publishes the
   * round it yields. A terminal `Take` becomes a terminal round and ends this
   * worker, which reports the terminal's cause, if any, as its sole reporter; a
   * data `Take` becomes the next round, which this worker then loops onto.
   */
  private def fetchAndPublish(round: Round[E, A]): ZIO[R, Nothing, Unit] = {
    release(round)
    fetch.flatMap { take =>
      take.exit.foldExit(
        cause =>
          Cause.flipCauseOption(cause) match {
            case None =>
              publish(round, Round.terminal[E, A])
            case Some(c) =>
              publish(round, Round.terminal[E, A]) *> onError(c)
          },
        chunk => {
          val nextRound = Round.data[E, A](chunk, n)
          publish(round, nextRound) *> loop(nextRound)
        }
      )
    }
  }

  /** Waits for the round another worker is fetching, then loops onto it. */
  private def awaitNext(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    round.next.await.flatMap(loopFn)

  /**
   * Claims from `round` until it runs out, running `f` on each claimed element,
   * then fetches or awaits the next round. A terminal round ends the worker
   * without reporting anything: its cause was reported by its fetcher.
   *
   * An `f` that returns an already-completed `Exit` (`Exit.unit`,
   * `Exit.succeed(a)`) is handled right here, in a plain JVM loop: its result is
   * known, so there is nothing to sequence and no effect node or closure to
   * build. Every other effect, `ZIO.succeed` and `ZIO.unit` included, goes back
   * to the interpreter through [[continueAfter]]. So the loop never recurses on
   * the JVM stack. A failure ends the worker's loop, abandoning the rest of the
   * claim, which is what fail-fast means here.
   *
   * This is not the `ZIO.whileLoop` version that was implemented and reverted
   * (it cut allocation by 23-38% while costing ~30% throughput): that one ran
   * every element through the interpreter. Hoisting the per-element closure
   * alone also measured as a no-op; what this removes for an `Exit` result is
   * the closure, the recursion and the interpreter round trip together.
   */
  def loop(round: Round[E, A]): ZIO[R, Nothing, Unit] = {
    var budget = Dispatcher.TrampolineEvery
    while (true) {
      if (round.terminal) return Exit.unit
      // Read the chunk once: the fetcher may null the field at any moment, and
      // the length checks and element reads must agree with each other.
      val chunk = round.chunk
      val length = if (chunk eq null) 0 else chunk.length
      val stride = round.stride
      // `getAndIncrement` rather than `getAndAdd(1)` at stride 1: it is a JIT
      // intrinsic with a dedicated code path, and stride 1 is the hot regime
      // whenever `f` is slow enough that a round is not batched.
      val i = if (stride == 1) round.cursor.getAndIncrement() else round.cursor.getAndAdd(stride)
      if (i >= length)
        return if (isFetcher(round, chunk, i, length, stride)) fetchAndPublish(round) else awaitNext(round)
      val until = if (stride == 1) i + 1 else (i + stride) min length
      var j = i
      while (j < until) {
        val effect = f(chunk(j))
        j += 1
        effect match {
          case _: Exit.Success[_] => ()
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
   *     invocations is the caller's responsibility, via scope interruption.
   *
   * The returned effect completes when every worker has observed a terminal
   * round.
   *
   * ==Starting the workers==
   *
   * All `n` workers begin on one shared seed round, and the election on it is
   * correct whenever each worker arrives. Two things about starting them do
   * matter:
   *
   *   - Each worker must make its own first claim when it runs. `loop` claims
   *     from the cursor as soon as it is called, so one evaluated `loop(seed)`
   *     handed to every worker would make each of them the seed's fetcher. The
   *     workers start from `start`, a suspended effect, which makes that
   *     impossible.
   *   - [[WorkerPool]] forks with `fork`, not `forkDaemon`, so the workers are
   *     children of the pool fiber and are interrupted with it when the caller
   *     closes the scope. Daemon workers would outlive a fail-fast teardown.
   */
  def run[R, E <: E1, E1, A](
    n: Int,
    fetch: ZIO[R, Nothing, Take[E, A]],
    f: A => ZIO[R, E1, Any],
    onError: Cause[E1] => ZIO[R, Nothing, Unit]
  )(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed(new Dispatcher[R, E, E1, A](n, fetch, f, onError).run)

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
