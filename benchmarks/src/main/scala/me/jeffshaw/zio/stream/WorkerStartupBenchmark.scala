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
 * Isolates the per-run cost of starting and stopping the worker pool, which is
 * what `WorkerPool` changed and which no existing benchmark measures.
 *
 * The cost being targeted is `O(n)` per *run*, not per element: the previous
 * implementation (`ZIO.foreachParDiscard(1 to n).withParallelism(n)`, resolving
 * to ZIO's `foreachParUnboundedDiscard`) retained a `Chunk` of all `n`
 * `Fiber.Runtime`s for the whole run, wrapped each worker in
 * `applyOnExitWith`, and walked every fiber calling `inheritAll` at shutdown.
 *
 * To make that visible, the *stream* is kept deliberately tiny while `n` grows.
 * Every other benchmark does the opposite — lots of elements, modest `n` — which
 * amortizes startup into invisibility. Here a run is dominated by pool setup and
 * teardown, so a change to either shows up directly.
 *
 * `elements` is the axis that separates the two readings:
 *
 *   - at 1 element, the score is essentially "runs per second of an `n`-worker
 *     pool that does nothing", i.e. pure startup and shutdown;
 *   - at 10000, real dispatch work is mixed in, showing how quickly the startup
 *     cost amortizes away.
 *
 * `f` is a no-op throughout, so nothing competes with the pool machinery.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class WorkerStartupBenchmark {

  /**
   * Worker count. The retained-fiber cost is linear in this, so the high end is
   * where a difference should appear; 4 is the control that should not move.
   */
  @Param(Array("4", "256", "4096", "16384"))
  var n: Int = _

  /**
   * How much work the pool has to do once started. 1 isolates startup and
   * teardown; 10000 shows how fast that cost amortizes.
   */
  @Param(Array("1", "10000"))
  var elements: Int = _

  @Param(Array("64"))
  var chunkSize: Int = _

  var chunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit = {
    val full = elements / chunkSize
    val rest = elements % chunkSize
    chunks =
      (0 until full).map(i => Chunk.fromArray(Array.fill(chunkSize)(i))) ++
        (if (rest > 0) Seq(Chunk.fromArray(Array.fill(rest)(0))) else Seq.empty)
  }

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(ZStream.fromChunks(chunks: _*).runForeachPar(n)(_ => Exit.unit))
    elements.toLong
  }
}
