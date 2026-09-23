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

import java.util.concurrent.atomic.AtomicInteger

/**
 * An alternative dispatcher that decouples the number of dispatch fibers from
 * `n` (Idea 1c in `OPTIMIZATION_IDEAS.md`).
 *
 * ==The cost it targets==
 *
 * [[Dispatcher]] runs `n` worker fibers, each applying `f` to one element at a
 * time, so `n` is simultaneously the concurrency bound on `f` '''and''' the size
 * of the dispatch pool. Every worker that loses fetcher election parks on
 * `Round.next`, and `Promise.complete` walks that waiter list serially on the
 * publishing fiber, so a round boundary costs O(waiters) on the critical path.
 *
 * `WakeHerdBenchmark` measures what that costs. With a cheap `f` it is severe: at
 * `chunkSize = 16`, throughput at `n = 4096` is 3.2% of the `n = 4` figure. With
 * a parking `f` it does not collapse, but it caps scaling: efficiency against the
 * Little's law ideal of `n / mean_latency` holds near 70% to `n = 64`, then falls
 * to 12.1% at `n = 1024` and 2.5% at `n = 4096`.
 *
 * ==The shape==
 *
 * `dispatchFibers` fibers claim elements from the shared cursor exactly as
 * before, but instead of applying `f` inline they '''fork''' it, having first
 * acquired one of `n` semaphore permits. Concurrency is then bounded by the
 * permit count rather than by the fiber count, and the waiter list on
 * `Round.next` is `dispatchFibers` long regardless of `n`.
 *
 * ==What this must not break==
 *
 * Two tests pin the concurrency contract, and they are what make this a
 * redesign rather than a tuning change:
 *
 *   - `parallelism is not exceeded` holds `n` invocations of `f` in a countdown
 *     latch and requires the count to reach zero, so at least `n` must run
 *     simultaneously, and no more.
 *   - `single chunk saturates all workers` does the same from a '''single'''
 *     chunk of `n` elements, so the `n` concurrent invocations have to come out
 *     of one round.
 *
 * So the permits cannot be a throttle over fewer runners: `n` real concurrent
 * `f` invocations must be reachable. Forking per element is what provides that,
 * and it is also the cost of this design, one fiber per element rather than one
 * per worker for the life of the run.
 *
 * Failure handling keeps [[Dispatcher]]'s contract. A forked `f` reports its own
 * cause through `onError`, and the permit is released on every exit path
 * including interruption, so a fail-fast teardown cannot leak permits and stall
 * the dispatchers.
 */
private[stream] final class ForkingDispatcher[R, E <: E1, E1, A](
  dispatchFibers: Int,
  permits: Queue[Unit],
  runScope: Scope,
  fetch: ZIO[R, Nothing, Take[E, A]],
  f: A => ZIO[R, E1, Any],
  onError: Cause[E1] => ZIO[R, Nothing, Unit]
)(implicit trace: Trace) {

  /**
   * Counts `f` invocations that have been forked but not yet finished, so the
   * run can wait for them after the rounds are exhausted. A fiber count rather
   * than a fiber collection, for the reason [[WorkerPool]] documents: retaining
   * `n` fibers is a cost this design is trying to avoid, not reproduce.
   */
  private[this] val inFlight = new AtomicInteger(0)

  private[this] val drained: Promise[Nothing, Unit] =
    Promise.unsafe.make[Nothing, Unit](FiberId.None)(Unsafe)

  val seed: Round[E, A] = Round.data[E, A](Chunk.empty, dispatchFibers)

  private def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] =
    round.next.done(Exit.succeed(next)).unit

  private def release(round: Round[E, A]): Unit =
    round.chunk = null.asInstanceOf[Chunk[A]]

  /** Set once every round has been consumed, so the last `f` can signal. */
  private[this] val roundsDone = new java.util.concurrent.atomic.AtomicBoolean(false)

  /**
   * Applies `f` to one element on a forked fiber, under a permit.
   *
   * ==Why a bounded `Queue[Unit]` and not a semaphore==
   *
   * The acquire and the release happen on '''different fibers''': the dispatcher
   * acquires, the forked child releases. That rules out ZIO's `Semaphore`
   * outright, since it exposes only scoped `withPermit*` forms which release when
   * the acquiring effect ends, and here that is the fork itself, so the bound
   * would be on forks issued rather than invocations in flight.
   *
   * A `java.util.concurrent.Semaphore` is the faster primitive and was tried.
   * ZIO 2.1.26's `Semaphore` allocates a `Promise` and boxes into an `Either` on
   * every acquire, contended or not, and measures 5 to 6 times slower than
   * Java's (zio/zio#11188 replaces it with an `AtomicLong` fast path; unreleased
   * as of 2.1.26), whereas `tryAcquire` is a single CAS that allocates nothing.
   * The blocking exposure was also acceptable: an acquire blocks only when `n`
   * invocations are in flight, and only dispatch fibers acquire, so at most
   * `dispatchFibers` threads can ever be parked, a small constant independent of
   * `n`.
   *
   * It still does not work, for a reason unrelated to throughput: a fail-fast
   * teardown cannot reclaim a dispatch fiber parked on a permit, and the run
   * hangs. `propagates error of original stream` and `a defect in the callback is
   * not swallowed` both time out once `f` is slow enough for permits to run out.
   *
   * The precise reason is worth stating, because it is easy to get backwards.
   * `Semaphore.acquire()` '''is''' interruptible in the Java sense: it throws
   * `InterruptedException` on a thread interrupt, and
   * `acquireUninterruptibly()` is the variant that ignores them. The gap is that
   * a ZIO '''fiber''' interrupt is not a JVM '''thread''' interrupt.
   * `ZIO.blocking` only shifts execution onto the blocking executor
   * (`ZIO.onExecutorWith`); nothing in it arranges for the carrier thread to be
   * interrupted. So the parked thread is interruptible in principle with nothing
   * to interrupt it.
   *
   * Polling `tryAcquire(1, MILLISECONDS)` in a loop restores interruptibility,
   * since the fiber becomes interruptible between attempts, but it introduces a
   * dependency on the '''real''' clock, which breaks the one test driven by
   * `TestClock.adjust`. A combinator that behaves differently under a test clock
   * is a worse defect than a slower permit.
   *
   * `Queue[Unit]` has neither problem. `take` is an ordinary interruptible
   * suspension with an uncontended `poll` fast path, and it involves no clock. It
   * costs an `uninterruptibleMask` and a `fiberIdWith` per element, which is the
   * price of correctness here.
   *
   * The release is in `ensuring`, so it runs on interruption too and a fail-fast
   * teardown cannot leak permits and stall the dispatchers.
   */
  private def dispatch(a: A): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed {
      permits.take *> ZIO.suspendSucceed {
        inFlight.incrementAndGet()
        f(a)
          .foldCauseZIO(onError, _ => Exit.unit)
          .ensuring(
            // The queue cannot be full: exactly `n` tokens exist and this fiber
            // holds one of them.
            permits.offer(()) *> ZIO.suspendSucceed {
              if (inFlight.decrementAndGet() == 0 && roundsDone.get) drained.done(Exit.unit).unit
              else Exit.unit
            }
          )
          // Forked into the run's scope, which is neither `forkDaemon` nor
          // `fork`. Both were measured and both are wrong:
          //
          //   - `forkDaemon` attaches to the global scope, so a fail-fast
          //     teardown never reaches these fibers and `f` keeps running.
          //     Three interruption tests fail, including `interrupts pending
          //     tasks` seeing 0 interruptions instead of 2. The same trap
          //     `WorkerPool` documents.
          //   - `fork` makes the child a child of the *dispatch fiber*, which
          //     finishes as soon as the rounds are exhausted and takes the
          //     in-flight `f` invocations down with it: 40 of 82 tests fail.
          //
          // The correct owner is the run, which outlives both.
          .forkIn(runScope)
          .unit
      }
    }

  private def fetchAndPublish(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    fetch.flatMap { take =>
      take.exit.foldExit(
        cause =>
          Cause.flipCauseOption(cause) match {
            case None => publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round))
            case Some(c) =>
              publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round)) *> onError(c)
          },
        chunk => {
          val nextRound = Round.data[E, A](chunk, dispatchFibers)
          publish(round, nextRound) *> ZIO.succeed(release(round)) *> loop(nextRound, 0)
        }
      )
    }

  private def awaitNext(round: Round[E, A]): ZIO[R, Nothing, Unit] =
    round.next.await.flatMap(loop(_, 0))

  private def isFetcher(round: Round[E, A], chunk: Chunk[A], i: Int, length: Int, stride: Int): Boolean =
    (chunk ne null) && (if (stride == 1) i == length else round.fetching.compareAndSet(false, true))

  private def runClaim(round: Round[E, A], chunk: Chunk[A], i: Int, until: Int, depth: Int): ZIO[R, Nothing, Unit] =
    dispatch(chunk(i)).flatMap { _ =>
      if (i + 1 < until) runClaim(round, chunk, i + 1, until, depth + 1)
      else loop(round, depth + 1)
    }

  def loop(round: Round[E, A], depth: Int): ZIO[R, Nothing, Unit] =
    if (round.terminal) Exit.unit
    else if (depth >= Dispatcher.TrampolineEvery) ZIO.suspendSucceed(loop(round, 0))
    else {
      val chunk = round.chunk
      val length = if (chunk eq null) 0 else chunk.length
      val stride = round.stride
      val i = if (stride == 1) round.cursor.getAndIncrement() else round.cursor.getAndAdd(stride)
      if (i < length)
        if (stride == 1) dispatch(chunk(i)).flatMap(_ => loop(round, depth + 1))
        else runClaim(round, chunk, i, (i + stride) min length, depth)
      else if (isFetcher(round, chunk, i, length, stride)) fetchAndPublish(round)
      else awaitNext(round)
    }

  /**
   * Runs the dispatch fibers, then waits for the forked `f` invocations.
   *
   * The second wait is what the inline design does not need: when `f` runs on
   * the worker itself, a worker finishing means its `f` finished. Here the
   * rounds can be exhausted while invocations are still in flight.
   */
  /**
   * Runs the dispatch fibers, then waits for the forked `f` invocations.
   *
   * The second wait is what the inline design does not need: when `f` runs on the
   * worker itself, a worker finishing means its `f` finished. Here the rounds can
   * be exhausted while invocations are still in flight.
   *
   * The handoff between "rounds are done" and "the last `f` finished" is the
   * subtle part, because both sides observe two variables. It is made safe by
   * having the completing side always win a tie: `roundsDone` is set first, then
   * `inFlight` is read, while a finishing `f` decrements `inFlight` first, then
   * reads `roundsDone`. With that ordering at least one of the two sees the other
   * side's write, so the promise is completed by somebody. Completing it twice is
   * harmless, since `Promise#done` on a completed promise is a no-op.
   */
  def run: ZIO[R, Nothing, Unit] =
    WorkerPool.replicate(dispatchFibers)(loop(seed, 0)) *>
      ZIO.suspendSucceed {
        roundsDone.set(true)
        if (inFlight.get == 0) {
          // Nothing in flight, so no `f` will ever signal. Complete it here.
          drained.done(Exit.unit) *> Exit.unit
        } else drained.await
      }
}

private[stream] object ForkingDispatcher {

  /**
   * How many fibers pull elements from the cursor, independent of `n`.
   *
   * Sized to the machine rather than to `n`, since dispatch is CPU work: a
   * dispatch fiber claims from an atomic, forks, and loops. More than a few per
   * core adds contention on the cursor without adding throughput, which is the
   * mistake this design exists to stop making.
   */
  def defaultDispatchFibers: Int =
    (java.lang.Runtime.getRuntime.availableProcessors * 2) max 2

  def run[R, E <: E1, E1, A](
    n: Int,
    fetch: ZIO[R, Nothing, Take[E, A]],
    f: A => ZIO[R, E1, Any],
    onError: Cause[E1] => ZIO[R, Nothing, Unit]
  )(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed {
      // Never more dispatch fibers than `n`, or the pool could hold more claims
      // than there are permits and the extra fibers would only block.
      val fibers = defaultDispatchFibers min n
      // `n` permit tokens. A scope of the run owns the forked `f` invocations, so
      // they outlive the dispatch fiber that started them but are still
      // interrupted when the run tears down.
      Queue.bounded[Unit](n).flatMap { permits =>
        permits.offerAll(Chunk.fill(n)(())) *>
          ZIO.scopedWith(scope => new ForkingDispatcher[R, E, E1, A](fibers, permits, scope, fetch, f, onError).run)
      }
    }
}
