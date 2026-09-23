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
 * Stresses the tail imbalance that `Round.MaxStride` exists to bound, using an
 * `f` whose cost varies between elements.
 *
 * ==Why this benchmark exists==
 *
 * `MaxStride` was raised from 16 to 256 on the strength of a +12.8% measurement
 * with a no-op `f`. The cap's stated purpose is to bound the tail: a worker
 * commits to `stride` elements before it can know whether it will be the round's
 * straggler, so the other workers may wait up to `(stride - 1) * cost(f)`. At 256
 * that exposure is sixteen times what it was.
 *
 * The check run at the time, `CrossoverBenchmark` at `fCostIters = 5000`, used a
 * '''uniform''' `f`. Uniform cost cannot produce a straggler, because every claim
 * is equally slow, so it does not test the cap's purpose at all. `Round.scala`
 * records an earlier stride experiment that measured 7 to 9% slower for exactly
 * this reason, which is what makes the gap worth closing.
 *
 * ==The shape of the skew==
 *
 * Slow elements are '''clustered''', not scattered. A claim of 256 contiguous
 * elements only becomes a straggler if the slow elements fall inside one claim;
 * spreading them uniformly would give every claim the same expected cost and
 * reproduce the uniform case. `slowRunLength` sets the cluster size, and
 * `slowEvery` how often a cluster appears.
 *
 * `costRatio` is the slow-to-fast cost ratio. A ratio of 1 is the uniform
 * control, which should show no effect from the cap at all.
 *
 * ==Calibration, which took two attempts==
 *
 * The costs have to be large relative to dispatch or this measures dispatch
 * instead of the tail. A first version used `fastCostIters = 200` with a nominal
 * 50x ratio, and the skew showed up as only 1.35x against the 4.07x the element
 * distribution implies. Timing the burn loop alone explained it: 200 iterations
 * costs about 5.5ns while per-element dispatch is roughly 19ns, so `f` was 22% of
 * the work and the mean could not move much whatever the tail did. The loop was
 * not being eliminated, merely dwarfed.
 *
 * Hence `fastCostIters = 20000` (about 284ns, roughly fifteen times dispatch)
 * and a 20x ratio (about 5.7us per slow element). A stride-256 claim landing
 * entirely on slow elements then serializes roughly 1.5ms, which is the tail the
 * cap exists to bound, while at stride 16 the same cluster spreads over sixteen
 * claims that peers can take.
 *
 * ==How to read it==
 *
 * Run against `MaxStride = 16` and `MaxStride = 256`. The prediction the cap
 * embodies is that 256 is worse here, and the size of that gap is what says
 * whether the +12.8% was bought at an unacceptable price. A configuration where
 * the stride is pinned to 1 regardless (`n` large relative to the round) is the
 * control that must not move.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class SkewedCostBenchmark {

  /**
   * Reduced from 200,000 because `fastCostIters` went up 100x: an operation has
   * to stay short enough to measure.
   */
  @Param(Array("20000"))
  var totalElements: Int = _

  /**
   * 512 with fusion gives a stride of 256 at `n = 4`, so the cap binds and the
   * old cap of 16 differs by the full sixteen times.
   */
  @Param(Array("512"))
  var chunkSize: Int = _

  /**
   * 4 is where the cap binds hardest. 256 pins the stride to 1 whatever the cap
   * is, so it is the control.
   */
  @Param(Array("4", "256"))
  var n: Int = _

  /**
   * Baseline per-element work, in multiply-add iterations.
   *
   * 20,000, not 200. Calibrated against a measured dispatch overhead of roughly
   * 19ns per element: at 200 iterations the loop costs about 5.5ns, so `f` was
   * only 22% of the work and the mean was dominated by dispatch rather than by
   * `f`, which is why a nominal 50x cost ratio showed up as 1.35x. At 20,000 the
   * fast element costs about 284ns, roughly fifteen times dispatch, so the tail
   * can actually dominate.
   */
  @Param(Array("20000"))
  var fastCostIters: Int = _

  /** How much more the slow elements cost. 1 is the uniform control. */
  @Param(Array("1", "20"))
  var costRatio: Int = _

  /**
   * Contiguous slow elements per cluster. Sized near the stride so a single
   * claim can be filled with slow work, which is the straggler the cap bounds.
   */
  @Param(Array("256"))
  var slowRunLength: Int = _

  /** One slow cluster per this many elements. */
  @Param(Array("4096"))
  var slowEvery: Int = _

  var chunks: IndexedSeq[Chunk[AnyRef]] = _

  /**
   * A distinct object per element, carrying its own cost.
   *
   * The elements are a reference type rather than `Int` because `f` is
   * `A => ZIO[R, E1, Any]`, so `A` erases to `Object` and an `Int` boxes on every
   * read; `ElementTypeBenchmark` measures that at about 11% of throughput, which
   * no production workload over a reference type pays. A small class is the
   * natural fit here since the element has to carry the fast/slow decision
   * anyway, and a distinct instance per element keeps the reads off a single
   * cache line.
   */
  final class Element(val iterations: Int)

  @Setup
  def setup(): Unit = {
    val fast = fastCostIters
    val slow = fastCostIters * costRatio
    val elems = Array.tabulate[AnyRef](totalElements) { i =>
      new Element(if ((i % slowEvery) < slowRunLength) slow else fast)
    }
    chunks = (0 until (totalElements / chunkSize)).map { c =>
      Chunk.fromArray(elems.slice(c * chunkSize, (c + 1) * chunkSize))
    }
  }

  @volatile var sink: Long = 0

  /**
   * Burns `iterations` multiply-adds seeded from `seed`, writing the result to a
   * `@volatile` field so the loop cannot be proved dead.
   *
   * The seed has to vary per call. An earlier version started from a literal, and
   * the JIT evidently collapsed the loop: a nominal 50x cost ratio produced a
   * measured mean multiplier of 1.24x against the 4.07x the element distribution
   * implies, so the "skewed" configuration was barely skewed and the benchmark was
   * not measuring what it claimed. `CrossoverBenchmark` seeds from the element
   * value for the same reason, and its sweep scales as expected.
   */
  private def burn(seed: Long, iterations: Int): Unit = {
    var acc = seed
    var i = 0
    while (i < iterations) {
      acc = acc * 6364136223846793005L + 1442695040888963407L
      i += 1
    }
    sink = acc
  }

  // The element carries its own iteration count, and its identity hash seeds the
  // burn loop so successive calls cannot share a folded result.
  private def callback: AnyRef => ZIO[Any, Nothing, Any] =
    e => {
      val el = e.asInstanceOf[Element]
      ZIO.succeed(burn(java.lang.System.identityHashCode(el).toLong, el.iterations))
    }

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(ZStream.fromChunks(chunks: _*).runForeachPar(n)(callback))
    totalElements.toLong
  }
}
