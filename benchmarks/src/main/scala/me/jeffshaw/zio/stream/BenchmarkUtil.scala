package me.jeffshaw.zio.stream

import zio._

/**
 * Minimal ZIO-only runtime helper for the JMH benchmarks: runs an effect to
 * completion on ZIO's default runtime, rethrowing its failure.
 *
 * `Runtime#unsafe` builds its API object on each call, which is one small
 * allocation per benchmark operation, and an operation here is a whole stream
 * run.
 */
object BenchmarkUtil {
  private val runtime = Runtime.default

  def unsafeRun[E, A](zio: ZIO[Any, E, A]): A =
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(zio).getOrThrowFiberFailure())
}
