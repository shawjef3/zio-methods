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
 * Sweeps `bufferSize`, the combinator's only tuning knob besides `n`, which
 * every other benchmark leaves at its default of 16.
 *
 * Two README claims are on trial here, both from "Sizing `bufferSize` and
 * chunks":
 *
 *   1. "going from one round per worker-pass to four matters and from forty to
 *      a hundred and sixty does not" — i.e. the return on `bufferSize` is steeply
 *      diminishing. The sweep is geometric (1, 4, 16, 64) so the early and late
 *      steps are the same multiple and directly comparable.
 *   2. "the queue only ever holds what the producer has actually produced [...]
 *      so raising `bufferSize` past what the source can stay ahead of buys
 *      nothing" — which is a claim about the *interaction* with producer speed,
 *      not about `bufferSize` alone. `producerCost` supplies the second axis:
 *      at 0 the in-memory source is fast and a large buffer can actually fill,
 *      while at 2000 the producer is the limiting stage and the claim predicts
 *      the `bufferSize` curve goes flat.
 *
 * `bufferSize = 1` is also the only configuration in the suite where
 * `BatchingFetch.fuse` never fuses: every round is a single chunk taken as-is.
 * The gap from 1 to 4 is therefore the measured value of the fusion path.
 *
 * `f` is a no-op and `totalElements` is fixed, so the score reflects dispatch
 * and transport only, and is comparable across every point.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
class BufferSizeBenchmark {

  /** Held constant so every point moves the same element count. */
  @Param(Array("200000"))
  var totalElements: Int = _

  /**
   * Small enough that `bufferSize` chunks is a meaningful fraction of the run,
   * so the fusion window has something to fuse.
   */
  @Param(Array("64"))
  var chunkSize: Int = _

  /**
   * Geometric, so "1 to 4" and "16 to 64" are the same multiple and the
   * diminishing-returns claim is a comparison of like with like. Four points
   * are enough for a claim about the *shape* of the curve: the first step
   * prices the fusion path, the last tests whether the curve has flattened.
   */
  @Param(Array("1", "4", "16", "64"))
  var bufferSize: Int = _

  /**
   * Per-chunk producer work, in multiply-add iterations. 0 is the in-memory
   * source the rest of the suite uses; 2000 makes the producer the limiting
   * stage (per `StreamParBenchmark`'s own calibration), which is the regime
   * where the README says a larger buffer stops helping.
   */
  @Param(Array("0", "2000"))
  var producerCost: Int = _

  /**
   * Pinned: the claims under test are about `bufferSize` against producer
   * speed, and `n` appears in neither. Sweeping it would multiply the point
   * count without addressing either claim.
   */
  @Param(Array("4"))
  var n: Int = _

  var chunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / chunkSize)).map(i => Chunk.fromArray(Array.fill(chunkSize)(i)))

  /** Consumes the producer work so the JIT cannot delete the loop. */
  @volatile var sink: Int = 0

  private def source: ZStream[Any, Nothing, Int] = {
    val base = ZStream.fromChunks(chunks: _*)
    if (producerCost == 0) base
    else
      base.mapChunks { chunk =>
        var acc = chunk.length
        var i = 0
        while (i < producerCost) {
          acc = acc * 31 + i
          i += 1
        }
        sink = acc
        chunk
      }
  }

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(source.runForeachPar(n, bufferSize)(_ => Exit.unit))
    totalElements.toLong
  }
}
