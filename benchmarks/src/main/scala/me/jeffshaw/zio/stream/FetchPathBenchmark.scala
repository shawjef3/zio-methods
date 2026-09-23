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
 * Isolates the per-round `fetch` path, which the other benchmarks only measure
 * incidentally.
 *
 * `fetch` runs once per round, not once per element, so its cost is visible
 * only when rounds are short relative to the work in them. The existing
 * benchmarks both hide it: `StreamParBenchmark` uses 50-element chunks but a
 * `bufferSize` large enough that fusion produces long rounds, and
 * `RealisticParBenchmark` has a 5ms `f` that swamps everything.
 *
 * The knob here is `chunkSize`. Small chunks mean many rounds over the same
 * element count, so per-round cost scales up against a fixed amount of work —
 * which is exactly the ratio a change to `fetch` moves. `f` is a no-op so
 * nothing else competes.
 *
 * The empty-queue case matters most: `takeBetween(1, max)` issues a
 * `takeUpTo(max)` that returns nothing and is discarded, then falls back to a
 * real `take`. Workers outrun an in-memory producer easily, so the steady state
 * here is an empty queue and that wasted poll on every round.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class FetchPathBenchmark {

  /** Held constant so every parameterization moves the same element count. */
  @Param(Array("200000"))
  var totalElements: Int = _

  /**
   * Rounds per run scale as `totalElements / chunkSize`, so the small sizes are
   * where per-round cost shows up. 1 is the pathological case a callback-driven
   * source produces.
   */
  @Param(Array("1", "8", "64", "512"))
  var chunkSize: Int = _

  @Param(Array("4", "64"))
  var n: Int = _

  // `AnyRef`, not `Int`: `f` is `A => ZIO[R, E1, Any]`, so `A` erases to `Object`
  // and an `Int` element boxes on every read. `ElementTypeBenchmark` measures that
  // at about 11% of throughput in this configuration, which is a cost no
  // production workload over a reference type pays. Distinct objects rather than
  // one repeated, so the reads do not all hit one cache line.
  var chunks: IndexedSeq[Chunk[AnyRef]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / chunkSize)).map(_ => Chunk.fromArray(Array.fill[AnyRef](chunkSize)(new AnyRef)))

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(ZStream.fromChunks(chunks: _*).runForeachPar(n)(_ => Exit.unit))
    totalElements.toLong
  }
}
