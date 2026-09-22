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
 * Tests the README's advice about reshaping the stream before handing it to
 * `runForeachPar`: the `grouped` idiom it recommends, and the `rechunk` it
 * mostly advises against.
 *
 * Both are claims about what a *caller* should write, which is why they belong
 * in one benchmark: the alternatives have to be measured against each other on
 * the same source, not each against a different baseline.
 *
 * == The `grouped` idiom ==
 *
 * The README recommends `stream.grouped(k).runForeachPar(n)(batch => ...)` to
 * get batching and concurrency together, and calls it "the one people reach
 * for". It is worth measuring because `grouped` emits one *element* per batch,
 * and the resulting stream's chunks are small — the shape
 * `FetchPathBenchmark` identifies as the pathological one for per-round cost.
 * So the recommended idiom may land in the worst dispatch regime, which is
 * exactly what a reader following the advice needs to know.
 *
 * `groupedRechunked` adds a `rechunk` after the `grouped`, which is the
 * mitigation the README's rechunk section would imply; `ungrouped` is the
 * control that does the same total per-element work with no batching at all,
 * establishing what the batching costs or saves in dispatch terms alone.
 *
 * To keep the comparison honest, all three do the same total work: `f` costs
 * `perElementCost` iterations per *element*, so a batch of `groupSize` costs
 * `groupSize` times as much. Only the dispatch shape differs.
 *
 * == `rechunk` ==
 *
 * "Usually no [...] worth rechunking only when the source emits pathologically
 * small chunks and `chunkSize * bufferSize < n`. Even then, raising
 * `bufferSize` is the cheaper fix."
 *
 * `sourceChunkSize` is the axis: 1 is the pathological callback-driven source,
 * 512 is a source that wants no reshaping. `rechunked` vs `asIs` measures
 * whether the copy pays for itself, and `asIsLargeBuffer` tests the "raising
 * `bufferSize` is the cheaper fix" half against the same source. At
 * `sourceChunkSize = 512` the README predicts `rechunked` is a pure loss.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class ChunkShapeBenchmark {

  @Param(Array("200000"))
  var totalElements: Int = _

  /**
   * The chunk size the source emits. 1 is the pathological case the README
   * names; 512 is a JDBC-cursor-shaped source it says to leave alone.
   */
  @Param(Array("1", "64", "512"))
  var sourceChunkSize: Int = _

  /** Batch size for the `grouped` idiom, matching the README's example. */
  @Param(Array("100"))
  var groupSize: Int = _

  @Param(Array("4", "32"))
  var n: Int = _

  /**
   * Per-element work in multiply-add iterations, chosen to sit just above the
   * crossover so parallelism is paying and dispatch is still visible. A no-op
   * `f` would make every variant a pure dispatch measurement and tell us
   * nothing about the batching advice, which is about amortizing real work.
   */
  @Param(Array("200"))
  var perElementCost: Int = _

  var chunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / sourceChunkSize))
      .map(i => Chunk.fromArray(Array.fill(sourceChunkSize)(i)))

  @volatile var sink: Long = 0

  private def burn(seed: Long, iterations: Int): Long = {
    var acc = seed
    var iter = 0
    while (iter < iterations) {
      acc = acc * 6364136223846793005L + 1442695040888963407L
      iter += 1
    }
    acc
  }

  /** Per-element `f`. */
  private val fElement: Int => ZIO[Any, Nothing, Any] = { i =>
    ZIO.succeed {
      val acc = burn(i.toLong, perElementCost)
      sink = acc
      acc
    }
  }

  /**
   * Per-batch `f`, costing `perElementCost` per element in the batch, so a run
   * over the whole stream does the same total work as `fElement` does.
   */
  private val fBatch: Chunk[Int] => ZIO[Any, Nothing, Any] = { batch =>
    ZIO.succeed {
      var acc = 0L
      var idx = 0
      while (idx < batch.length) {
        acc = burn(acc + batch(idx).toLong, perElementCost)
        idx += 1
      }
      sink = acc
      acc
    }
  }

  private def source: ZStream[Any, Nothing, Int] = ZStream.fromChunks(chunks: _*)

  // --- The `grouped` idiom -------------------------------------------------

  /** The README's recommended shape for batching with concurrency. */
  @Benchmark
  def groupedBatched: Long = {
    unsafeRun(source.grouped(groupSize).runForeachPar(n)(fBatch))
    totalElements.toLong
  }

  /**
   * The same idiom with the small chunks `grouped` produces repaired, which is
   * what the rechunk section would suggest for a source shaped like this.
   */
  @Benchmark
  def groupedBatchedRechunked: Long = {
    unsafeRun(source.grouped(groupSize).rechunk(64).runForeachPar(n)(fBatch))
    totalElements.toLong
  }

  // --- `rechunk` ------------------------------------------------------------

  /**
   * The source as it comes, default `bufferSize`.
   *
   * Serves both groups: it is the "usually no" baseline for the rechunk claim,
   * and the no-batching control for the `grouped` idiom — identical total work,
   * so the gap to `groupedBatched` is what the batching shape costs or saves in
   * dispatch alone. (The real-world payoff of batching, fewer round trips, is
   * not something an in-memory benchmark can model.)
   */
  @Benchmark
  def asIs: Long = {
    unsafeRun(source.runForeachPar(n)(fElement))
    totalElements.toLong
  }

  /** Pay the copy to repair chunk size. */
  @Benchmark
  def rechunked: Long = {
    unsafeRun(source.rechunk(512).runForeachPar(n)(fElement))
    totalElements.toLong
  }

  /**
   * The README's claimed cheaper fix for the same problem: leave the chunks
   * alone and widen the fusion window instead.
   */
  @Benchmark
  def asIsLargeBuffer: Long = {
    unsafeRun(source.runForeachPar(n, 256)(fElement))
    totalElements.toLong
  }
}
