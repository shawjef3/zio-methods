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
 * Isolates the cost of the round-publish wake, which no existing benchmark
 * separates from everything else `n` affects.
 *
 * ==What is being tested==
 *
 * `Round.next` is a `Promise`. Every worker that loses fetcher election parks on
 * it, and `Promise.complete` walks the resulting linked list of waiters
 * '''serially, on the publishing fiber''', before that fiber can run any `f`.
 * So a round boundary is O(waiters) work on the critical path.
 *
 * A round of `length` elements at stride `s` can occupy at most
 * `ceil(length / s)` workers. Every worker beyond that is woken, finds the
 * cursor exhausted, and re-parks: a full wake and a fresh `asyncInterrupt`
 * registration to accomplish nothing.
 *
 * ==How the two are separated==
 *
 * `n` is swept while the work is held fixed, past the point where more workers
 * can help. With a no-op `f` on a 4-core host, nothing above a handful of
 * workers can add throughput, so any change past that point is coordination
 * cost rather than parallelism. If the wake walk is the dominant term,
 * throughput should fall roughly linearly in `n`; if it is not, the curve should
 * flatten instead.
 *
 * `elementsPerRound` is the second axis, and it is what makes this diagnostic
 * rather than merely suggestive. It changes how many workers a round can occupy
 * without changing `n`:
 *
 *   - `chunkSize = 16` fuses to short rounds, so at `n = 4096` nearly every
 *     worker wakes for nothing. Maximum waste.
 *   - `chunkSize = 2048` fuses to long rounds that can occupy far more workers,
 *     so the same `n` wastes proportionally fewer wakes.
 *
 * The prediction that distinguishes the wake herd from generic fiber overhead:
 * the penalty for raising `n` should be '''much steeper at small chunk sizes'''.
 * If instead the two chunk sizes degrade alike, the cost is something about
 * having many fibers rather than about waking them per round, and ideas 1a/1b
 * (which only reduce wake cost) would not repay their complexity.
 *
 * `f` is a no-op so nothing competes with the coordination being measured.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 3)
class WakeHerdBenchmark {

  /**
   * Elements per operation.
   *
   * Held constant within an `fCost`, never across one: a costly `f` has to run
   * over fewer elements or an operation takes seconds. At 200 microseconds per
   * element with `n = 4`, 200,000 elements is ten seconds per op, which is
   * unusable. The runner therefore pairs each `fCost` with a suitable count, and
   * scores are only ever compared within a single `fCost` and `totalElements`
   * pair. The comparison that matters is always a ratio against `n = 4` inside
   * one such pair, so differing element counts across pairs are harmless.
   */
  @Param(Array("200000"))
  var totalElements: Int = _

  /**
   * Sets how many workers a round can occupy, via the round length fusion
   * produces. Small chunks mean short rounds and mostly-wasted wakes.
   */
  @Param(Array("16", "2048"))
  var chunkSize: Int = _

  /**
   * Swept far past what a no-op `f` on 4 cores can use. 4 is the reference
   * point; everything above it adds coordination without adding parallelism.
   */
  @Param(Array("4", "64", "1024", "4096", "16384"))
  var n: Int = _

  /**
   * Per-element cost of `f`, which decides whether the wake cost matters.
   *
   * The no-op sweep above establishes that the wake collapse is real, but with
   * a no-op `f` coordination is 100% of the work, so it says nothing about how
   * much the collapse costs a real workload. This axis finds where it stops
   * mattering.
   *
   *   - `noop`: coordination is everything. The upper bound on the effect.
   *   - `spin1us`, `spin10us`: on-CPU work. Note this competes for the same 4
   *     cores as the dispatch machinery, so it conflates "work dominates
   *     coordination" with "cores are saturated"; read it alongside the parked
   *     rows rather than alone.
   *   - `park200us`: `ZIO.sleep`, which yields the core. This is the shape the
   *     README's target workload has (a database round trip, an HTTP call), and
   *     the one that decides whether ideas 1a/1b/1c repay their complexity.
   *     Scaled down from the README's 5 ms so a run finishes.
   */
  @Param(Array("noop", "spin1us", "spin10us", "park200us"))
  var fCost: String = _

  var chunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    chunks = (0 until (totalElements / chunkSize)).map(i => Chunk.fromArray(Array.fill(chunkSize)(i)))

  @volatile var sink: Long = 0

  /** Busy-waits roughly `nanos`, consuming the result so the JIT keeps it. */
  private def spin(nanos: Long): Unit = {
    val deadline = java.lang.System.nanoTime() + nanos
    var acc = 1L
    while (java.lang.System.nanoTime() < deadline)
      acc = acc * 6364136223846793005L + 1442695040888963407L
    sink = acc
  }

  private def callback: Int => ZIO[Any, Nothing, Any] =
    fCost match {
      case "noop" => _ => Exit.unit
      case "spin1us" => _ => ZIO.succeed(spin(1000L))
      case "spin10us" => _ => ZIO.succeed(spin(10000L))
      case "park200us" => _ => ZIO.sleep(200.micros)
      case other => throw new IllegalArgumentException(s"unknown fCost: $other")
    }

  @Benchmark
  def runForeachPar: Long = {
    unsafeRun(ZStream.fromChunks(chunks: _*).runForeachPar(n)(callback))
    totalElements.toLong
  }
}
