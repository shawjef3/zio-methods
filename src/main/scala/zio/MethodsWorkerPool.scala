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

package zio

import zio.stacktracer.TracingImplicits.disableAutoTrace

/**
 * Runs `n` copies of one effect concurrently, completing when the last of them
 * finishes.
 *
 * This replaces `ZIO.foreachParDiscard(1 to n)(...).withParallelism(n)` in
 * [[me.jeffshaw.zio.stream.ChunkCursorDistributor]], and is derived from the
 * implementation that call resolves to: ZIO 2.1.26's private
 * `ZIO.foreachParUnboundedDiscard`, selected because `parallelism == size`.
 *
 * ==What it drops, and why that is worth a copy==
 *
 * The library version keeps a `Chunk` of all `n` `Fiber.Runtime`s and captures
 * it in the continuation of `promise.await`, so every fiber object stays
 * reachable for the whole run. `runForeachPar` advertises `n` in the tens of
 * thousands, so that is up to ~40,960 retained fibers, an `applyOnExitWith`
 * wrapper apiece, and an `inheritAll` per fiber at shutdown.
 *
 * It needs that collection because it forks each worker with `forkDaemon`, onto
 * the '''global''' scope. Nothing else can then interrupt them, so it holds
 * every fiber and interrupts them by hand on failure.
 *
 * Forking into the current fiber's scope instead makes interruption structural:
 * interrupting this fiber, which is what `runForeachPar` does when it closes
 * the scope this runs in, interrupts the workers with it. No collection has to
 * be retained, nothing needs interrupting by hand, and there is no shutdown
 * walk. `transplant`/`Grafter` goes with it, since its role was to give the
 * daemon fibers' ''children'' a scope to be transferred to on exit, which a
 * normally-forked worker's children already have.
 *
 * That the interruption really is structural is a test rather than an argument:
 * `RunForeachParSpec`'s "interrupts pending tasks when one of the tasks fails"
 * counts interruptions, and sees 0 instead of 2 if the workers are forked as
 * daemons without the retained collection.
 *
 * ==Why `package zio`==
 *
 * `Promise.unsafe` is `private[zio]`. Being in the package let this start as a
 * faithful copy, with pieces removed one at a time and the tests run after
 * each, rather than reconstructing the behavior from guesses — which had
 * already failed four times against a protocol whose startup precondition is
 * documented on [[me.jeffshaw.zio.stream.ChunkCursorDistributor.run]]. The
 * bisection is what identified `forkDaemon`, rather than anything about
 * scheduling, as the part that mattered.
 *
 * Nothing outside this object depends on that access, and the only remaining
 * use of it is `promise.unsafe.done`, which avoids an `Exit` allocation per
 * worker exit.
 */
object MethodsWorkerPool {

  /**
   * Forks `n` copies of `worker` and completes when the last one exits.
   *
   * The workers are forked into the calling fiber's scope, so interrupting the
   * returned effect interrupts all of them.
   */
  def replicate[R, E](n: Int)(worker: => ZIO[R, E, Any])(implicit trace: Trace): ZIO[R, E, Unit] =
    n match {
      case 0 => Exit.unit
      // As in the library version: a single worker needs no fork at all, and
      // runs on the calling fiber.
      case 1 => worker.unit
      case size =>
        ZIO.uninterruptibleMask { restore =>
          val promise = Promise.unsafe.make[Unit, Unit](FiberId.None)(Unsafe)
          val remaining = new java.util.concurrent.atomic.AtomicInteger(size)

          ZIO.foreachDiscard(0 until size) { _ =>
            restore(worker)
              .ensuring(ZIO.succeed {
                if (remaining.decrementAndGet() == 0) promise.unsafe.done(Exit.unit)(Unsafe)
              })
              .fork
          } *>
            // `ensuring` runs on interruption too, so the count reaches zero
            // whether the workers complete or are interrupted, and a fail-fast
            // teardown cannot leave this await hanging.
            //
            // The promise keeps the library's `Promise[Unit, Unit]` shape, but
            // nothing here ever fails it: the countdown is the only writer and
            // it only writes success. The `orDieWith` is a type-level formality
            // over an uninhabited failure.
            restore(promise.await)
              .orDieWith(_ => new IllegalStateException("runForeachPar worker pool failed unexpectedly"))
        }
    }
}
