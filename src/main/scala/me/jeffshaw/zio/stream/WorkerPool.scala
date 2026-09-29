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

import zio._
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicInteger

/**
 * Runs `n` copies of one effect concurrently, completing when the last of them
 * finishes.
 *
 * This replaces `ZIO.foreachParDiscard(1 to n)(...).withParallelism(n)` in
 * [[Dispatcher.run]], and is derived from the implementation that call
 * resolves to: ZIO 2.1.26's private `ZIO.foreachParUnboundedDiscard`, selected
 * because `parallelism == size`.
 *
 * ==What it drops, and why that is worth doing by hand==
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
 * ==How this was arrived at==
 *
 * Four hand-written fork loops failed the suite before this. What worked was
 * copying `foreachParUnboundedDiscard` verbatim into `package zio`, where its
 * `private[zio]` dependencies are reachable, confirming the copy passed the
 * suite, then removing one piece at a time and re-running.
 *
 * Those failures were once blamed on `forkDaemon` breaking fetcher election on
 * the shared seed round. That cannot be the mechanism: the election is correct
 * in any arrival order (see the seed in [[Dispatcher]]). A more likely cause is
 * that `Dispatcher.loop` claims from the cursor when it is called, so a loop
 * that built the worker effect once and forked it `n` times made every worker
 * the seed's fetcher. `worker` is by-name here, and [[Dispatcher.run]] passes a
 * suspended effect, so neither can happen now. The fork mode matters only for
 * interruption, as above.
 *
 * ==What it is worth==
 *
 * Retention, not throughput. Measured against the previous implementation with
 * `WorkerStartupBenchmark`, whose deliberately tiny stream leaves a run
 * dominated by pool setup and teardown: every point from `n = 4` to
 * `n = 16384` had overlapping error bars, and the one apparent separation
 * (-14% at `n = 16384` over 10,000 elements) fell to -4.6% when re-measured
 * alone at `-f 5 -wi 10 -i 10`, with the baseline side then showing a fork that
 * never reached steady state. Forking `n` fibers dominates either way; not
 * retaining them afterwards does not make the forking faster.
 *
 * So this exists to stop holding ~40,960 fiber objects for the length of a run,
 * and that is not something the benchmarks measure. `RetentionSpec` covers
 * element and round retention, not fibers, so nothing currently guards it.
 *
 * ==Reduction to public API==
 *
 * The copy has since been reduced to public API and moved here. Two
 * substitutions were needed, both off the per-element path:
 *
 *   - `Promise.make` in place of `Promise.unsafe.make`, which is public but
 *     would need an `Unsafe` in scope. This is one effect per run.
 *   - `Promise#done` in place of the `private[zio]` `promise.unsafe.done`.
 *     `done(io)` is `ZIO.succeed(unsafe.completeWith(io))`, so the work is
 *     identical and the cost is one effect node, once per run: only the final
 *     worker to exit completes the promise.
 */
private[stream] object WorkerPool {

  /**
   * Forks `n` copies of `worker` and completes when the last one exits. As in
   * the library version, a single worker is not forked at all and runs on the
   * calling fiber.
   *
   * The workers are forked into the calling fiber's scope, so interrupting the
   * returned effect interrupts all of them. Each worker counts itself down in
   * `ensuring`, which runs on interruption too, so a fail-fast teardown cannot
   * leave the final await hanging. The countdown only ever writes success, so
   * its promise is `Promise[Nothing, Unit]` rather than the library's
   * `Promise[Unit, Unit]`, and the await needs no fold.
   */
  def replicate[R, E](n: Int)(worker: => ZIO[R, E, Any])(implicit trace: Trace): ZIO[R, E, Unit] =
    n match {
      case 0 => Exit.unit
      case 1 => worker.unit
      case workers =>
        ZIO.uninterruptibleMask { restore =>
          Promise.make[Nothing, Unit].flatMap { allDone =>
            val remaining = new AtomicInteger(workers)
            val signalIfLast = allDone.done(Exit.unit).when(remaining.decrementAndGet() == 0)

            ZIO.foreachDiscard(0 until workers) { _ =>
              restore(worker).ensuring(signalIfLast).fork
            } *>
              restore(allDone.await)
          }
        }
    }
}
