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

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio._
import zio.stream._

import java.util.concurrent.TimeUnit

import me.jeffshaw.zio.stream.BenchmarkUtil._

/**
 * Tests the README's claim that `n == 1` is a useful configuration rather than
 * a degenerate one:
 *
 *   "`n == 1` does *not* [degrade to sequential]: it keeps the worker topology,
 *   so the stream is still consumed concurrently with `f`."
 *
 * and, from the scaladoc, "a slow producer and a slow `f` overlap".
 *
 * The claim is a trade, and both halves need measuring. Keeping the topology
 * costs a queue, a producer fiber and a worker fiber; it buys producer/`f`
 * overlap. So `n == 1` should *lose* to `runForeach` when the producer is free
 * (cost with nothing to overlap) and *win* when the producer is slow enough
 * that the overlap covers it.
 *
 * `producerParks` is the axis that decides it, and it matters for the same
 * reason `StreamParBenchmark` keeps both a slow and a blocking upstream: an
 * on-CPU producer and a parked one behave differently. With only 4 cores on the
 * measurement host, a CPU-bound producer overlapping a CPU-bound `f` competes
 * for the same core and the overlap is partly fictional, whereas a parked
 * producer genuinely leaves the core to `f`. Measuring both is what separates
 * "the topology overlaps I/O" from "the topology overlaps computation".
 *
 * `runForeachParN1` vs `runForeachSequential` is the comparison; `n == 0` is
 * included as a control, since it is documented to delegate to `runForeach` and
 * should therefore be indistinguishable from it. A gap there would mean the
 * delegation costs something.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class SingleWorkerBenchmark {

  @Param(Array("50000"))
  var totalElements: Int = _

  @Param(Array("64"))
  var chunkSize: Int = _

  /**
   * Per-chunk producer cost in multiply-add iterations. 0 is the free in-memory
   * source, where the topology has nothing to overlap and should be pure
   * overhead; 2000 makes the producer the limiting stage.
   */
  @Param(Array("0", "2000"))
  var producerCost: Int = _

  /**
   * Whether the producer's cost is spent parked or on-CPU. Parked is the shape
   * where the overlap is real on a core-constrained host; on-CPU is where it
   * competes with `f`. Ignored when `producerCost` is 0.
   */
  @Param(Array("true", "false"))
  var producerParks: Boolean = _

  /**
   * How long a parked producer sleeps per chunk. Set well above timer
   * granularity so the score reflects the requested wait rather than the
   * scheduler's resolution. Only used when `producerParks` and `producerCost`
   * is non-zero.
   */
  @Param(Array("200"))
  var parkedSleepMicros: Long = _

  /** Per-element `f` cost, high enough that there is something to overlap. */
  @Param(Array("500"))
  var fCostIters: Int = _

  var chunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / chunkSize)).map(i => Chunk.fromArray(Array.fill(chunkSize)(i)))

  @volatile var sink: Long = 0

  private val f: Int => ZIO[Any, Nothing, Any] = { i =>
    ZIO.succeed {
      var acc = i.toLong
      var iter = 0
      while (iter < fCostIters) {
        acc = acc * 6364136223846793005L + 1442695040888963407L
        iter += 1
      }
      sink = acc
      acc
    }
  }

  /**
   * The source. When `producerParks`, the per-chunk cost is a `ZIO.sleep`;
   * otherwise it is a spin loop inside `mapChunks`.
   *
   * The two are not calibrated to equal wall time and cannot be: a sleep short
   * enough to match the spin loop would be dominated by timer granularity
   * rather than by the requested duration. `parkedSleepMicros` sets the parked
   * duration independently, so the comparison to draw is `n == 1` against
   * `runForeach` *within* a producer shape, never a score from one shape
   * against a score from the other.
   */
  private def source: ZStream[Any, Nothing, Int] = {
    val base = ZStream.fromChunks(chunks: _*)
    if (producerCost == 0) base
    else if (producerParks)
      base.mapChunksZIO(chunk => ZIO.sleep(Duration.fromNanos(parkedSleepMicros * 1000L)).as(chunk))
    else
      base.mapChunks { chunk =>
        var acc = chunk.length
        var i = 0
        while (i < producerCost) {
          acc = acc * 31 + i
          i += 1
        }
        sink = acc.toInt
        chunk
      }
  }

  @Benchmark
  def runForeachParN1: Long = {
    unsafeRun(source.runForeachPar(1)(f))
    totalElements.toLong
  }

  @Benchmark
  def runForeachSequential: Long = {
    unsafeRun(source.runForeach(f))
    totalElements.toLong
  }

  /** Control: documented to delegate to `runForeach`, so it should tie with it. */
  @Benchmark
  def runForeachParN0: Long = {
    unsafeRun(source.runForeachPar(0)(f))
    totalElements.toLong
  }
}
