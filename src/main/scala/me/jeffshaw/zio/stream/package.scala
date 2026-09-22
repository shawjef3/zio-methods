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

package me.jeffshaw.zio

import zio._
import zio.stream.{Take, ZStream}
import zio.stacktracer.TracingImplicits.disableAutoTrace

package object stream {

  /**
   * Adds `runForeachPar` to [[zio.stream.ZStream]] as an extension method,
   * since it cannot be added as a member of the published `ZStream` class.
   */
  implicit final class ZStreamMethods[-R, +E, +A](private val self: ZStream[R, E, A]) extends AnyVal {

    /**
     * Consumes all elements of the stream, passing them to the specified
     * callback, executing up to `n` invocations of `f` concurrently. The element
     * order is not enforced by this combinator.
     *
     * Unlike [[zio.stream.ZStream#mapZIOParUnordered]] followed by
     * [[zio.stream.ZStream#runDrain]], this combinator does not emit the results
     * of `f` downstream, and so avoids the overhead of buffering and re-chunking
     * them. Prefer it when the results of `f` are not needed.
     *
     * If any invocation of `f` fails, the remaining in-flight invocations are
     * interrupted and the returned effect fails. Because interruption is not
     * instantaneous, more than one failure can be recorded before the workers
     * stop; the returned effect fails with all of them combined, rather than
     * with only the first.
     */
    def runForeachPar[R1 <: R, E1 >: E](n: => Int)(f: A => ZIO[R1, E1, Any])(implicit
      trace: Trace
    ): ZIO[R1, E1, Unit] =
      runForeachPar[R1, E1](n, 16)(f)

    /**
     * Consumes all elements of the stream, passing them to the specified
     * callback, executing up to `n` invocations of `f` concurrently. The element
     * order is not enforced by this combinator.
     *
     * Unlike [[zio.stream.ZStream#mapZIOParUnordered]] followed by
     * [[zio.stream.ZStream#runDrain]], this combinator does not emit the results
     * of `f` downstream, and so avoids the overhead of buffering and re-chunking
     * them. Prefer it when the results of `f` are not needed.
     *
     * Rather than forking a fiber per element, this combinator forks `n`
     * long-lived worker fibers that pull elements from a shared buffer of up to
     * `bufferSize` elements. This bounds the concurrency globally, without a
     * barrier at chunk boundaries, so a slow invocation of `f` never leaves the
     * other workers idle while elements remain.
     *
     * `n == 1` still uses that topology: exactly one invocation of `f` runs at a
     * time, but the stream continues to be consumed into the buffer while `f`
     * runs, so a slow producer and a slow `f` overlap. Only a non-positive `n`
     * degrades to sequential consumption, in which the stream is pulled and `f`
     * applied on a single fiber and `bufferSize` has no effect.
     *
     * If any invocation of `f` fails, the remaining in-flight invocations are
     * interrupted and the returned effect fails. Because interruption is not
     * instantaneous, more than one failure can be recorded before the workers
     * stop; every recorded failure is combined with [[zio.Cause.Both]], so the
     * returned effect fails with all of them rather than with only the first.
     *
     * A failure of the underlying stream is recorded once, regardless of `n`.
     * This matches [[zio.stream.ZStream#mapZIOParUnordered]], which accumulates
     * concurrent failures the same way.
     */
    def runForeachPar[R1 <: R, E1 >: E](n: => Int, bufferSize: => Int)(f: A => ZIO[R1, E1, Any])(implicit
      trace: Trace
    ): ZIO[R1, E1, Unit] =
      ZIO.suspendSucceed {
        val nn = n
        val bufferSizeV = bufferSize
        // Only a non-positive `n` falls back to sequential consumption, where
        // "no workers" has no sensible forked meaning. `n == 1` takes the normal
        // forked path: it means "one element at a time", not "no pipelining".
        // `runForeach` would interleave pulling and `f` on a single fiber, so a
        // stream with real producer latency would stall while `f` runs, and
        // `bufferSize` would be silently ignored.
        if (nn <= 0) self.runForeach(f)
        else
          ZIO.scopedWith { scope =>
            for {
              // Chunk-granular transport: `Take`s move through the queue (one box
              // per chunk, not per element), while workers dispatch individual
              // elements out of the current chunk via a shared atomic cursor
              // (`ChunkCursorDistributor`). This bounds concurrency at the element
              // level — matching `mapZIOParUnordered` — without a chunk boundary
              // barrier, and without the per-element `Exit.Success` boxing of an
              // element-granular queue.
              queue <- Queue.bounded[Take[E, A]](bufferSizeV)
              _ <- scope.addFinalizer(queue.shutdown)
              childScope <- scope.fork
              fiberId <- ZIO.fiberId
              // Holds whether the run failed, why, and the fail-fast signal, as
              // one mechanism. See `FailureAccumulator` for the invariants that
              // keeping them together enforces, in particular that an
              // interruption-only cause fires the signal without being recorded.
              failures <- FailureAccumulator.make[E1]
              // Producer: feed the stream's chunks into the queue as `Take`s,
              // terminated by `Take.end` on end-of-stream or `Take.failCause` on
              // error.
              _ <- self
                .runIntoQueueScoped(queue)
                .provideSomeEnvironment[R1](_.add[Scope](childScope))
                .forkIn(childScope)
              // Batched fetch: one round spans every chunk already buffered
              // rather than exactly one, which keeps the round boundary rare,
              // along with the wake-herd it causes when `n` exceeds the chunk size.
              // See `BatchingFetch` for why, and for the terminal parking that
              // makes a batch containing end-of-stream safe. Built once per run.
              fetch = BatchingFetch.effect[E, A](queue, bufferSizeV)
              // `n` workers claim elements from the shared cursor and apply `f`. A
              // worker that finishes an element immediately claims the next, so
              // there is no barrier between chunks; a single chunk keeps all `n`
              // workers busy.
              worker = ChunkCursorDistributor.run[R1, E, E1, A](nn, fetch, f, failures.record)
              workerFiber <- worker.forkIn(childScope)
              // Wait for the workers to finish, unless a failure signals
              // fail-fast first, in which case interrupt them.
              _ <- workerFiber.join.raceFirst(failures.await)
              _ <- childScope.close(Exit.interrupt(fiberId))
              // `Exit.unit` when the run never failed, otherwise the accumulated
              // cause, which is empty for an interruption-only failure.
              _ <- failures.result.flatten
            } yield ()
          }
      }
  }
}
