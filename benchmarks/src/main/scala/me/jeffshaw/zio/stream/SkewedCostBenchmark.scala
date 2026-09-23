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

  @Param(Array("200000"))
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

  /** Baseline per-element work, in multiply-add iterations. */
  @Param(Array("200"))
  var fastCostIters: Int = _

  /** How much more the slow elements cost. 1 is the uniform control. */
  @Param(Array("1", "50"))
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

  var chunks: IndexedSeq[Chunk[Int]] = _

  /**
   * Elements carry their own cost, so `f` needs no index arithmetic and cannot
   * be accidentally uniform. A positive value is fast, negative is slow.
   */
  @Setup
  def setup(): Unit = {
    val costs = Array.tabulate(totalElements) { i =>
      val posInCycle = i % slowEvery
      if (posInCycle < slowRunLength) -1 else 1
    }
    chunks = (0 until (totalElements / chunkSize)).map { c =>
      Chunk.fromArray(costs.slice(c * chunkSize, (c + 1) * chunkSize))
    }
  }

  @volatile var sink: Long = 0

  private def burn(iterations: Int): Unit = {
    var acc = 1L
    var i = 0
    while (i < iterations) {
      acc = acc * 6364136223846793005L + 1442695040888963407L
      i += 1
    }
    sink = acc
  }

  private def callback: Int => ZIO[Any, Nothing, Any] = {
    val fast = fastCostIters
    val slow = fastCostIters * costRatio
    marker => ZIO.succeed(burn(if (marker < 0) slow else fast))
  }

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(ZStream.fromChunks(chunks: _*).runForeachPar(n)(callback))
    totalElements.toLong
  }
}
