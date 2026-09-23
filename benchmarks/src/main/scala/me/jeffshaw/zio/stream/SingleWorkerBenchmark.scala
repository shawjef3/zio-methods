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
 * `producer` is the axis that decides it, and it distinguishes three shapes for
 * the same reason `StreamParBenchmark` keeps both a slow and a blocking
 * upstream: an on-CPU producer and a parked one behave differently. On a
 * core-constrained host a CPU-bound producer overlapping a CPU-bound `f`
 * competes for the same core, so the overlap is partly fictional, whereas a
 * parked producer genuinely leaves the core to `f`. Measuring both is what
 * separates "the topology overlaps I/O" from "the topology overlaps
 * computation".
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
@Fork(value = 2)
class SingleWorkerBenchmark {

  @Param(Array("50000"))
  var totalElements: Int = _

  @Param(Array("64"))
  var chunkSize: Int = _

  /**
   * The producer shape, as one parameter rather than a cost/parks cross: with
   * two parameters, every `parks` value at zero cost names the same
   * configuration, and JMH would measure that duplicate as if it were a
   * distinct point.
   *
   *   - `free`   — in-memory source. The topology has nothing to overlap, so
   *                `n == 1` should be pure overhead against `runForeach`.
   *   - `onCpu`  — per-chunk spin. Overlap exists but competes for the same
   *                core, which on a 4-core host makes it partly fictional.
   *   - `parked` — per-chunk `ZIO.sleep`. The producer genuinely yields the
   *                core, so this is where the claimed overlap should pay.
   */
  @Param(Array("free", "onCpu", "parked"))
  var producer: String = _

  /**
   * Per-chunk spin iterations for the `onCpu` producer. Calibrated by
   * `StreamParBenchmark` to make the producer the limiting stage.
   */
  @Param(Array("2000"))
  var onCpuIters: Int = _

  /**
   * How long the `parked` producer sleeps per chunk. Set well above timer
   * granularity so the score reflects the requested wait rather than the
   * scheduler's resolution.
   */
  @Param(Array("200"))
  var parkedSleepMicros: Long = _

  /** Per-element `f` cost, high enough that there is something to overlap. */
  @Param(Array("500"))
  var fCostIters: Int = _

  var chunks: IndexedSeq[Chunk[AnyRef]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / chunkSize)).map(_ => Chunk.fromArray(Array.fill[AnyRef](chunkSize)(new AnyRef)))

  @volatile var sink: Long = 0

  private val f: AnyRef => ZIO[Any, Nothing, Any] = { e =>
    ZIO.succeed {
      var acc = java.lang.System.identityHashCode(e).toLong
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
  private def source: ZStream[Any, Nothing, AnyRef] = {
    val base = ZStream.fromChunks(chunks: _*)
    producer match {
      case "free" => base
      case "parked" =>
        base.mapChunksZIO(chunk => ZIO.sleep(Duration.fromNanos(parkedSleepMicros * 1000L)).as(chunk))
      case "onCpu" =>
        base.mapChunks { chunk =>
          var acc = chunk.length
          var i = 0
          while (i < onCpuIters) {
            acc = acc * 31 + i
            i += 1
          }
          sink = acc.toLong
          chunk
        }
      case other => throw new IllegalArgumentException(s"unknown producer shape: $other")
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
