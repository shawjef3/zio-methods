package me.jeffshaw.zio.methods

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio._
import zio.stream._

import java.util.concurrent.TimeUnit

import me.jeffshaw.zio.methods.BenchmarkUtil._

/**
 * Models the target production scenario: plenty of chunks already available,
 * chunks of hundreds to thousands of elements, and an `f` that takes ~5ms per
 * element. In this regime per-element combinator overhead (~tens of ns) is
 * noise; what decides throughput is whether all `n` workers stay saturated.
 *
 * Ideal throughput for IO-bound `f` is n / 5ms = 200*n elements/s. The score
 * of interest is measured elements/s vs that ideal (worker-saturation
 * efficiency).
 *
 * `f` is modeled two ways:
 *   - `sleep`: `ZIO.sleep(5.millis)` — an async wait (IO-ish). Exercises the
 *     ZIO timer/scheduler wake-up path at high fiber counts.
 *   - `spin`: 5ms of CPU burn — a compute-bound `f`. Ideal is bounded by
 *     physical cores regardless of `n`.
 */
@State(JScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 2)
@Warmup(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 2)
@Fork(value = 1)
class RealisticParBenchmark {

  @Param(Array("100"))
  var chunkCount: Int = _

  @Param(Array("2000"))
  var chunkSize: Int = _

  // The target scenario is tens of thousands of concurrent IO operations, so
  // n runs far past chunkSize: at n > chunkSize each round has fewer elements
  // than workers, stressing the await/wake path and cursor contention.
  @Param(Array("256", "2048", "16384", "40960"))
  var n: Int = _

  var zioChunks: IndexedSeq[Chunk[Int]] = _

  @Setup
  def setup(): Unit =
    zioChunks = (1 to chunkCount).map(i => Chunk.fromArray(Array.fill(chunkSize)(i)))

  @volatile var sink: Long = 0

  private val fSleep: Int => ZIO[Any, Nothing, Any] =
    _ => ZIO.sleep(5.millis)

  private def fSpin: Int => ZIO[Any, Nothing, Any] = { i =>
    ZIO.succeed {
      val deadline = java.lang.System.nanoTime() + 5_000_000L
      var acc      = i.toLong
      while (java.lang.System.nanoTime() < deadline)
        acc = acc * 6364136223846793005L + 1442695040888963407L
      sink = acc
      acc
    }
  }

  @Benchmark
  def runForeachParSleep: Long = {
    unsafeRun(ZStream.fromChunks(zioChunks: _*).runForeachPar(n)(fSleep))
    chunkCount.toLong * chunkSize
  }

  @Benchmark
  def mapZIOParUnorderedDrainSleep: Long = {
    unsafeRun(ZStream.fromChunks(zioChunks: _*).mapZIOParUnordered(n)(fSleep).runDrain)
    chunkCount.toLong * chunkSize
  }

  @Benchmark
  def runForeachParSpin: Long = {
    unsafeRun(ZStream.fromChunks(zioChunks: _*).runForeachPar(n)(fSpin))
    chunkCount.toLong * chunkSize
  }

  /**
   * Control: the same total work and concurrency bound with no stream and no
   * distributor — `foreachParDiscard` straight over the elements. This is the
   * ZIO-runtime-native ceiling for "run 200k 5ms sleeps, at most n at once".
   * If this plateaus at the same rate as `runForeachParSleep`, the bottleneck
   * is the runtime's fiber/timer machinery, not the chunk-cursor protocol.
   */
  @Benchmark
  def pureForeachParDiscardSleep: Long = {
    unsafeRun(
      ZIO
        .foreachParDiscard(1 to (chunkCount * chunkSize))(_ => ZIO.sleep(5.millis))
        .withParallelism(n)
    )
    chunkCount.toLong * chunkSize
  }
}
