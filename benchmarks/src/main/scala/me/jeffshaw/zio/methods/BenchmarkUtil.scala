package me.jeffshaw.zio.methods

import zio._

/**
 * Minimal ZIO-only runtime helper for the JMH benchmarks, mirroring the parts
 * of ZIO's own `zio.BenchmarkUtil` that the `runForeachPar` benchmarks use.
 */
object BenchmarkUtil extends Runtime[Any] { self =>
  val environment  = Runtime.default.environment
  val fiberRefs    = Runtime.default.fiberRefs
  val runtimeFlags = Runtime.default.runtimeFlags

  override val unsafe = super.unsafe

  def unsafeRun[E, A](zio: ZIO[Any, E, A]): A =
    Unsafe.unsafe(implicit unsafe => self.unsafe.run(zio).getOrThrowFiberFailure())
}
