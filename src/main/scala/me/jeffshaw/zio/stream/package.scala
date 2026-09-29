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
     * The overload that also takes `bufferSize`, called here with 16, documents
     * how the work is distributed and how failures are reported.
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
     * `bufferSize` chunks of the stream. This bounds the concurrency globally,
     * without a barrier at chunk boundaries, so a slow invocation of `f` never
     * leaves the other workers idle while elements remain.
     *
     * `bufferSize` counts chunks, not elements, so the elements buffered are up
     * to `bufferSize` times the stream's chunk size. It is not a quantity to
     * compare against `n`: what should exceed `n` is that product, so that the
     * buffered chunks give every worker several elements.
     *
     * `n == 1` still uses that topology: exactly one invocation of `f` runs at a
     * time, but the stream continues to be consumed into the buffer while `f`
     * runs, so a slow producer and a slow `f` overlap. Only a non-positive `n`
     * degrades to sequential consumption, in which the stream is pulled and `f`
     * applied on a single fiber and `bufferSize` has no effect. A non-positive
     * `bufferSize` is treated as 1 rather than rejected, in the same way that
     * [[zio.ZIO.foreachParDiscard]] runs sequentially for a non-positive
     * parallelism rather than failing.
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
        // Clamped once, for the queue and the fetcher alike: `Queue.bounded`
        // dies on a non-positive capacity.
        val bufferSizeV = bufferSize max 1
        if (nn <= 0) self.runForeach(f)
        else
          ZIO.scopedWith { scope =>
            for {
              queue <- Queue.bounded[Take[E, A]](bufferSizeV)
              _ <- scope.addFinalizer(queue.shutdown)
              childScope <- scope.fork
              fiberId <- ZIO.fiberId
              failures <- FailureAccumulator.make[E1]
              // The producer: the stream's chunks go into the queue as `Take`s,
              // ended by `Take.end` or by `Take.failCause` with the stream's own
              // failure, defect or interruption. `runForeachChunk` rather than
              // `runIntoQueueScoped`, which layers a writer channel, a per-chunk
              // `mapOutZIO` and a `drain` over the same work; with small chunks
              // the producer bounds the run. Interruption of this fiber by the
              // scope never reaches the handler.
              _ <- self
                .runForeachChunk(chunk => queue.offer(Take.chunk(chunk)))
                .foldCauseZIO(cause => queue.offer(Take.failCause(cause)), _ => queue.offer(Take.end))
                .forkIn(childScope)
              fetch = BatchingFetch.effect[E, A](queue, bufferSizeV, nn)
              worker = ChunkCursorDistributor.run[R1, E, E1, A](nn, fetch, f, failures.record)
              workerFiber <- worker.forkIn(childScope)
              // Whichever comes first, the workers finishing or a recorded
              // failure; closing the scope then interrupts whatever still runs.
              _ <- workerFiber.join.raceFirst(failures.await)
              _ <- childScope.close(Exit.interrupt(fiberId))
              _ <- failures.result.flatten
            } yield ()
          }
      }
  }
}
