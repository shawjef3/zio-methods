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
 * Locates the `f` cost at which parallelism starts paying, so the README's
 * selection guide can name a number rather than gesture at one.
 *
 * The other benchmarks fix `f` and vary the combinator. This one fixes the
 * combinators — `runForeachPar` against sequential `runForeach` — and sweeps the
 * per-element cost of `f`, because the crossover is the only thing a caller
 * choosing between them actually needs to know.
 *
 * `fCostIters` is a count of multiply-add iterations, not a time: it maps to
 * roughly a nanosecond apiece on the machine this was calibrated on. The
 * accumulator escapes to a `@volatile` field so the JIT cannot delete the loop.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
class CrossoverBenchmark {

  @Param(Array("2000"))
  var chunkCount: Int = _

  @Param(Array("50"))
  var chunkSize: Int = _

  /**
   * Per-element work in the callback, in multiply-add iterations.
   *
   * 10 and 50 are dropped from the original sweep: they sit inside the flat
   * region below the crossover, where the README claims nothing beyond
   * "sequential wins", and the four remaining points already bracket the
   * crossover on both sides.
   */
  @Param(Array("0", "200", "1000", "5000"))
  var fCostIters: Int = _

  /**
   * The README presents the crossover ("between 200 and 1,000 iterations") as a
   * general figure, but it was only ever swept at `n = 4`. The break-even
   * should move with `n`: more workers divide the work further, which pulls the
   * crossover down, while also costing more coordination, which pushes it up.
   * Sweeping `n` is what turns the table into a claim about the combinator
   * rather than about one configuration of it.
   */
  @Param(Array("2", "4", "32"))
  var n: Int = _

  var zioChunks: IndexedSeq[Chunk[AnyRef]] = _

  @Setup
  def setup(): Unit =
    zioChunks = (1 to chunkCount).map(_ => Chunk.fromArray(Array.fill[AnyRef](chunkSize)(new AnyRef)))

  @volatile var sink: Long = 0

  /**
   * Seeds the burn loop from the element's identity hash rather than its value.
   *
   * The elements are `AnyRef` rather than `Int` because `f` is
   * `A => ZIO[R, E1, Any]`, so `A` erases to `Object` and an `Int` boxes on every
   * read; `ElementTypeBenchmark` measures that at about 11% of throughput, which
   * no production workload over a reference type pays. The seed still has to vary
   * per element so the JIT cannot fold the loop, and `identityHashCode` is a
   * cheap varying value with no allocation.
   */
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

  @Benchmark
  def par: Long = {
    unsafeRun(ZStream.fromChunks(zioChunks: _*).runForeachPar(n)(f))
    chunkCount.toLong * chunkSize
  }

  /**
   * The sequential baseline does not use `n`, so sweeping `n` would re-measure
   * an identical configuration once per value. Pin it with `-p n=4` when
   * running the sweep, and compare every `par` point against that one baseline.
   */
  @Benchmark
  def sequential: Long = {
    unsafeRun(ZStream.fromChunks(zioChunks: _*).runForeach(f))
    chunkCount.toLong * chunkSize
  }
}
