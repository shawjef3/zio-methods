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
 * Tests the `grouped` idiom the README recommends for getting batching and
 * concurrency together:
 *
 *   "regroup so each element is itself a batch and hand that to
 *   `runForeachPar` — then `n` bounds concurrent *batches*:
 *   `stream.grouped(100).runForeachPar(4)(batch => insertAll(batch))`"
 *
 * It is called "the one people reach for", and nothing measured it. It is
 * worth measuring because `grouped` emits one *element* per batch, so the
 * stream handed to `runForeachPar` has small chunks — the shape
 * `FetchPathBenchmark` identifies as worst for per-round cost. The recommended
 * idiom may therefore land in the worst dispatch regime, which is precisely
 * what a reader following the advice needs to know.
 *
 * The source chunk size is fixed: what varies here is the *shape the caller
 * builds*, not the shape the source emits, which `ChunkShapeBenchmark` covers.
 *
 * All three variants do the same total work — `f` costs `perElementCost` per
 * element, so a batch of `groupSize` costs `groupSize` times as much — so the
 * only difference is dispatch. What this cannot model is the real-world payoff
 * of batching, which is fewer round trips to a remote resource; the point of
 * the control is to price the dispatch side, so that benefit can be weighed
 * against a known cost rather than an assumed one.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
class GroupedBatchBenchmark {

  @Param(Array("200000"))
  var totalElements: Int = _

  /** A source that needs no reshaping, so the idiom is the only variable. */
  @Param(Array("512"))
  var sourceChunkSize: Int = _

  /** Batch size, matching the README's example. */
  @Param(Array("100"))
  var groupSize: Int = _

  @Param(Array("4"))
  var n: Int = _

  @Param(Array("200"))
  var perElementCost: Int = _

  var chunks: IndexedSeq[Chunk[AnyRef]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / sourceChunkSize))
      .map(_ => Chunk.fromArray(Array.fill[AnyRef](sourceChunkSize)(new AnyRef)))

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

  private val fElement: AnyRef => ZIO[Any, Nothing, Any] = { e =>
    ZIO.succeed {
      val acc = burn(java.lang.System.identityHashCode(e).toLong, perElementCost)
      sink = acc
      acc
    }
  }

  /** Costs `perElementCost` per element of the batch, matching `fElement`. */
  private val fBatch: Chunk[AnyRef] => ZIO[Any, Nothing, Any] = { batch =>
    ZIO.succeed {
      var acc = 0L
      var idx = 0
      while (idx < batch.length) {
        acc = burn(acc + java.lang.System.identityHashCode(batch(idx)).toLong, perElementCost)
        idx += 1
      }
      sink = acc
      acc
    }
  }

  private def source: ZStream[Any, Nothing, AnyRef] = ZStream.fromChunks(chunks: _*)

  /** The README's recommended shape. */
  @Benchmark
  def grouped: Long = {
    unsafeRun(source.grouped(groupSize).runForeachPar(n)(fBatch))
    totalElements.toLong
  }

  /**
   * The same idiom with the small chunks `grouped` produces repaired. If the
   * idiom does land in the bad dispatch regime, this is the fix, and the gap
   * measures what the regime costs.
   */
  @Benchmark
  def groupedRechunked: Long = {
    unsafeRun(source.grouped(groupSize).rechunk(64).runForeachPar(n)(fBatch))
    totalElements.toLong
  }

  /** Control: same total work, no batching. Prices the batching shape itself. */
  @Benchmark
  def ungrouped: Long = {
    unsafeRun(source.runForeachPar(n)(fElement))
    totalElements.toLong
  }
}
