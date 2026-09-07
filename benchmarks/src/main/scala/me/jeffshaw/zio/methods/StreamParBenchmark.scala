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

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio._
import zio.stream._

import java.util.concurrent.TimeUnit

import me.jeffshaw.zio.methods.BenchmarkUtil._

@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 1)
class StreamParBenchmark {

  @Param(Array("10000"))
  var chunkCount: Int = _

  @Param(Array("50"))
  var parChunkSize: Int = _

  var zioChunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    zioChunks = (1 to chunkCount).map(i => Chunk.fromArray(Array.fill(parChunkSize)(i)))

  @Benchmark
  def zioRunForeachPar: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeachPar(4)(i => ZIO.succeed(BigDecimal.valueOf(i.toLong).pow(3)))

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // Baseline for `runForeachPar` using only pre-existing combinators: run the
  // effects of each chunk in parallel, discarding the results. Parallelism is
  // bounded per chunk and there is a barrier at each chunk boundary.
  @Benchmark
  def zioRunForeachChunkForeachParDiscard: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeachChunk { chunk =>
        ZIO
          .foreachParDiscard(chunk)(i => ZIO.succeed(BigDecimal.valueOf(i.toLong).pow(3)))
          .withParallelism(4)
      }

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // Baseline for `runForeachPar` using `mapZIOParUnordered` followed by a drain:
  // this streams the results of `f` through a buffer and re-chunks them, which
  // `runForeachPar` avoids.
  @Benchmark
  def zioMapZIOParUnorderedDrain: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .mapZIOParUnordered(4)(i => ZIO.succeed(BigDecimal.valueOf(i.toLong).pow(3)))
      .runDrain

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // Baseline for `runForeachPar` using the ordered `mapZIOPar` followed by a
  // drain: like `zioMapZIOParUnorderedDrain`, but additionally pays for
  // preserving element order.
  @Benchmark
  def zioMapZIOParDrain: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .mapZIOPar(4)(i => ZIO.succeed(BigDecimal.valueOf(i.toLong).pow(3)))
      .runDrain

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // Sequential baseline: no parallelism at all. Establishes the floor of
  // per-element overhead against which the parallel variants can be compared.
  @Benchmark
  def zioRunForeachSequential: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeach(i => ZIO.succeed(BigDecimal.valueOf(i.toLong).pow(3)))

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // ---------------------------------------------------------------------------
  // Zero-cost-`f` variants. `f` does no work, so what remains is the overhead of
  // the combinator machinery itself. These are the variants to profile (e.g.
  // `-prof gc`) to find allocation hotspots in the combinators, not in `f`.
  // ---------------------------------------------------------------------------

  @Benchmark
  def zioRunForeachParNoop: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeachPar(4)(_ => Exit.unit)

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  @Benchmark
  def zioRunForeachChunkForeachParDiscardNoop: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeachChunk { chunk =>
        ZIO.foreachParDiscard(chunk)(_ => Exit.unit).withParallelism(4)
      }

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  @Benchmark
  def zioMapZIOParUnorderedDrainNoop: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .mapZIOParUnordered(4)(_ => Exit.unit)
      .runDrain

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  @Benchmark
  def zioRunForeachSequentialNoop: Long = {
    val result = ZStream
      .fromChunks(zioChunks: _*)
      .runForeach(_ => Exit.unit)

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // ---------------------------------------------------------------------------
  // Slow-upstream variant: a source with real per-chunk CPU cost.
  //
  // The benchmarks above draw from `ZStream.fromChunks`, an in-memory source
  // with near-zero production cost. `upstreamCost` adds real per-chunk work in
  // the *producer*, in a `mapChunks` stage, so it runs on whichever fiber pulls
  // the stream.
  //
  // `upstreamCost` throttles the run from the producer side: measured at
  // n = 4, throughput falls monotonically with it (226 / 137 / 57 / 7.1 ops/s
  // at 0 / 200 / 2000 / 20000), so even the default 200 already makes the
  // producer the limiting stage rather than the workers.
  //
  // What this does NOT reproduce is the queue-less design's collapse, which
  // needs a pull that *suspends* — see the blocking-upstream benchmark below.
  // A slow on-CPU pull and a parked one both keep the fetcher from running `f`,
  // but only the parked one costs a scheduler wake to resume, and only it was
  // measured to sink the queue-less shape. Keeping both benchmarks is what
  // separates "producer is slow" from "producer parks".
  // ---------------------------------------------------------------------------

  @Param(Array("0", "50", "200"))
  var upstreamCost: Int = _

  /** Consumes the producer work so it cannot be optimized away. */
  @volatile var sink: Int = 0

  /**
   * Per-chunk producer work. The accumulator is consumed via `sink` (a
   * `@volatile` field) so the JIT cannot prove it dead and delete the loop.
   */
  private def slowUpstream(s: ZStream[Any, Nothing, Int]): ZStream[Any, Nothing, Int] =
    s.mapChunks { chunk =>
      var acc = chunk.length
      var i   = 0
      while (i < upstreamCost) {
        acc = acc * 31 + i
        i += 1
      }
      sink = acc
      chunk
    }

  @Benchmark
  def zioRunForeachParSlowUpstream: Long = {
    val result = slowUpstream(ZStream.fromChunks(zioChunks: _*))
      .runForeachPar(4)(_ => Exit.unit)

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }

  // ---------------------------------------------------------------------------
  // Blocking-upstream variant: the case the CPU-bound sweep above cannot reach,
  // and a regression guard for the producer-fiber + queue design.
  //
  // A source that cannot keep up with the workers is the precondition, and
  // `slowUpstream` above already supplies it — its producer is the limiting
  // stage from `upstreamCost = 200` on. What that benchmark lacks is a pull
  // that *suspends*: its cost is burned on-CPU, so the pulling fiber is busy
  // rather than parked, and it does not reproduce the queue-less collapse.
  //
  // This benchmark adds the parking. A suspended pull costs a scheduler wake to
  // resume on top of the wait itself, and that is the shape where the producer
  // fiber and queue pay for themselves, by keeping up to `bufferSize` of work in
  // flight across the wait.
  //
  // A queue-less design in which the fetcher is itself a worker (pulling the
  // stream directly; measured and rejected) loses ~44% throughput here: while
  // that worker waits on the pull, nothing is queued behind it, and once the
  // current chunk drains the remaining workers idle until the pull returns.
  // Keep this benchmark as the guard against reintroducing that shape.
  //
  // The source is a bounded `Queue` fed by a forked producer, so `queue.take`
  // genuinely suspends the fiber rather than spinning.
  //
  // `producerDelayNanos` sets how long the producer waits between chunks. It is
  // spent in `ZIO.sleep`, which parks rather than spins; 0 means "as fast as the
  // producer can offer", which still parks the consumer whenever it out-runs it.
  // ---------------------------------------------------------------------------

  @Param(Array("0", "10000", "100000"))
  var producerDelayNanos: Long = _

  /**
   * A stream backed by a bounded queue fed by a separate fiber, so pulling
   * parks the puller when the queue is empty. `bufferChunks` is deliberately
   * small so the consumer can out-run the producer and actually block.
   */
  private def blockingUpstream: ZStream[Any, Nothing, Int] = {
    val bufferChunks = 4
    ZStream.unwrapScoped {
      for {
        q <- Queue.bounded[Take[Nothing, Int]](bufferChunks)
        producer = ZIO.foreachDiscard(zioChunks) { chunk =>
                     val offer = q.offer(Take.chunk(chunk))
                     if (producerDelayNanos == 0L) offer
                     else ZIO.sleep(Duration.fromNanos(producerDelayNanos)) *> offer
                   } *> q.offer(Take.end)
        _ <- producer.forkScoped
      } yield ZStream.fromQueue(q).flattenTake
    }
  }

  @Benchmark
  def zioRunForeachParBlockingUpstream: Long = {
    val result = blockingUpstream.runForeachPar(4)(_ => Exit.unit)

    unsafeRun(result)
    zioChunks.length.toLong * parChunkSize
  }
}
