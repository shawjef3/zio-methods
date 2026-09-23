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
 * How much of the measured dispatch cost is boxing, and does changing the
 * element type remove it?
 *
 * Profiling `FetchPathBenchmark` found `BoxesRunTime.boxToInteger` at 23% of CPU
 * and 1.59 MB allocated per operation, about 8 bytes per element. The cause is
 * that `f` has type `A => ZIO[R, E1, Any]`, so `A` erases to `Object` and handing
 * an `Int` to `f` must box. Every benchmark in this project uses `Chunk[Int]`, so
 * every cheap-`f` result carries that cost, while a production workload over a
 * reference type does not.
 *
 * This measures the four candidate element types against each other with an
 * identical no-op `f`, so the only difference is what the dispatch loop reads out
 * of the chunk.
 *
 *   - `int`: `Chunk[Int]`, what the benchmarks use today. Boxes on every read.
 *   - `boxed`: `Chunk[Integer]`, pre-boxed and '''outside''' the -128..127
 *     `Integer` cache, so each element is a distinct object. No boxing on read,
 *     but the elements are scattered across the heap, which is what a real
 *     workload looks like.
 *   - `shared`: `Chunk[AnyRef]` where every slot is the '''same''' object. No
 *     boxing and no allocation, but a single hot cache line, which is why this is
 *     a floor rather than a realistic figure: real elements are not all the same
 *     pointer.
 *   - `unit`: `Chunk[Unit]`, the degenerate case. Scala represents this as
 *     `BoxedUnit.UNIT` everywhere, so it is `shared` by another name and is
 *     included only to confirm that.
 *
 * The gap between `int` and `boxed` is what the current benchmarks are
 * overstating. The gap between `boxed` and `shared` is how much of the remainder
 * is memory locality rather than dispatch, which is why `shared` should not be
 * adopted as the standard element type even though it will score best.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class ElementTypeBenchmark {

  @Param(Array("200000"))
  var totalElements: Int = _

  @Param(Array("512"))
  var chunkSize: Int = _

  @Param(Array("4"))
  var n: Int = _

  @Param(Array("int", "boxed", "shared", "unit"))
  var elementType: String = _

  var intChunks: IndexedSeq[Chunk[Int]] = _
  var refChunks: IndexedSeq[Chunk[AnyRef]] = _

  @Setup
  def setup(): Unit = {
    val count = totalElements / chunkSize
    elementType match {
      case "int" =>
        intChunks = (0 until count).map(c => Chunk.fromArray(Array.tabulate(chunkSize)(i => c * chunkSize + i)))
      case "boxed" =>
        // Offset past the Integer cache so every element is a distinct object,
        // which is the point: a cached Integer would share one cache line and
        // quietly measure `shared` instead.
        refChunks = (0 until count).map { c =>
          Chunk.fromArray(Array.tabulate[AnyRef](chunkSize)(i => Integer.valueOf(1000 + c * chunkSize + i)))
        }
      case "shared" =>
        val one = new Object
        refChunks = (0 until count).map(_ => Chunk.fromArray(Array.fill[AnyRef](chunkSize)(one)))
      case "unit" =>
        // `scala.runtime.BoxedUnit.UNIT` is a singleton, so this is `shared`
        // under another name; included to confirm that rather than assume it.
        val u: AnyRef = scala.runtime.BoxedUnit.UNIT
        refChunks = (0 until count).map(_ => Chunk.fromArray(Array.fill[AnyRef](chunkSize)(u)))
      case other =>
        throw new IllegalArgumentException(s"unknown elementType: $other")
    }
  }

  @Benchmark
  def runForeachPar: Long = {
    if (elementType == "int") unsafeRun(ZStream.fromChunks(intChunks: _*).runForeachPar(n)(_ => Exit.unit))
    else unsafeRun(ZStream.fromChunks(refChunks: _*).runForeachPar(n)(_ => Exit.unit))
    totalElements.toLong
  }
}
