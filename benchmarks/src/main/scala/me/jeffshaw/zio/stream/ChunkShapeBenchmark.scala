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
 * Tests the README's advice against `rechunk`:
 *
 *   "Usually no [...] It is worth rechunking only when the source emits
 *   pathologically small chunks (one element per callback, say) *and*
 *   `chunkSize * bufferSize < n`. Even then, raising `bufferSize` is the
 *   cheaper fix, because `rechunk` copies every element."
 *
 * Two separable claims, and `sourceChunkSize` is the axis for both.
 * `rechunked` against `asIs` asks whether the copy ever pays for itself;
 * `asIsLargeBuffer` against `rechunked` asks whether the claimed cheaper fix is
 * in fact cheaper, on the same source. At `sourceChunkSize = 512` the README
 * predicts `rechunked` is a pure loss, and at 1 it predicts the opposite
 * ordering, with `asIsLargeBuffer` ahead.
 *
 * `n` is pinned: the advice is about repairing chunk shape, and the condition
 * it names (`chunkSize * bufferSize < n`) is moved here by `sourceChunkSize`
 * and `bufferSize`, both of which vary, rather than by `n`.
 *
 * `f` costs `perElementCost` iterations per element, just above the crossover,
 * so parallelism is paying and dispatch is still visible. A no-op `f` would
 * make this a pure dispatch measurement and say nothing about the advice as a
 * caller would experience it.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
class ChunkShapeBenchmark {

  @Param(Array("200000"))
  var totalElements: Int = _

  /**
   * The chunk size the source emits. 1 is the pathological callback-driven
   * case the README names; 512 is a JDBC-cursor-shaped source it says to leave
   * alone; 64 sits between them.
   */
  @Param(Array("1", "64", "512"))
  var sourceChunkSize: Int = _

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

  private val f: AnyRef => ZIO[Any, Nothing, Any] = { e =>
    ZIO.succeed {
      var acc = java.lang.System.identityHashCode(e).toLong
      var iter = 0
      while (iter < perElementCost) {
        acc = acc * 6364136223846793005L + 1442695040888963407L
        iter += 1
      }
      sink = acc
      acc
    }
  }

  private def source: ZStream[Any, Nothing, AnyRef] = ZStream.fromChunks(chunks: _*)

  /** The source as it comes, default `bufferSize`. The "usually no" baseline. */
  @Benchmark
  def asIs: Long = {
    unsafeRun(source.runForeachPar(n)(f))
    totalElements.toLong
  }

  /** Pay the copy to repair chunk size. */
  @Benchmark
  def rechunked: Long = {
    unsafeRun(source.rechunk(512).runForeachPar(n)(f))
    totalElements.toLong
  }

  /**
   * The README's claimed cheaper fix for the same problem: leave the chunks
   * alone and widen the fusion window instead.
   */
  @Benchmark
  def asIsLargeBuffer: Long = {
    unsafeRun(source.runForeachPar(n, 256)(f))
    totalElements.toLong
  }
}
