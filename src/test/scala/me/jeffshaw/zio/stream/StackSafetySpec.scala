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

import java.util.concurrent.{CompletableFuture, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicIntegerArray}

import zio._
import zio.stream._
import zio.test._

/**
 * Guards the dispatch loop against stack overflow when `f` returns an `Exit`.
 *
 * `Dispatcher.loop` handles an `f` that returns an already-completed `Exit`
 * (such as `_ => Exit.unit`) inline, without going back to the ZIO interpreter.
 * If that path ever recursed rather than looping, as it once did through
 * `Exit#foldCauseZIO`, its JVM stack depth would be the length of a round,
 * which neither `Round.MaxClaimSize` nor fusion bounds. Other synchronous effects,
 * `ZIO.succeed` among them, are evaluated by the interpreter's own loop and do
 * not recurse, so only an `Exit` result exercises this.
 *
 * Every run here happens on a dedicated [[SmallStackRuntime]], whose threads all
 * have [[SmallStackBytes]] of stack. Starting the run from a small-stack thread
 * is not enough: `runForeachPar` forks its workers, and a forked fiber runs on
 * its runtime's executor, so with the default runtime the recursion would
 * happen on ZIO's scheduler threads, whose stack is whatever `-Xss` the JVM got
 * (4MB under the sbt launcher), deep enough to hide the bug. A stack overflow is
 * a fatal error to ZIO, which by default prints it and exits the JVM, so the
 * runtime instead records it as the run's outcome.
 */
object StackSafetySpec extends ZIOSpecDefault {

  /**
   * The stack size of every thread a [[SmallStackRuntime]] runs fibers on.
   *
   * 1MB sits between measurements taken on JDK 25. The flat loop passes every
   * test here with 128KB, so it has 8x headroom and a failure means real
   * recursion. A loop that recursed once per element but trampolined every 512
   * elements (commit e4df319) needed more than 384KB and passed with 512KB, so
   * recursion bounded by a small constant still passes with room to spare. The
   * same loop with the trampoline disabled overflowed in every test at 256KB,
   * 512KB and 1MB. At 2MB the two 200,000-element single-chunk tests still
   * overflowed but the other two, whose rounds are shorter, did not, and even
   * at 1MB the many-chunks test caught it in only two runs of three until it
   * was given a deeper buffer. So much above 1MB the guard would weaken, and
   * much below it the guard would start to reject bounded recursion that is
   * harmless.
   */
  private val SmallStackBytes = 1024L * 1024

  /** How many small-stack threads each run's executor has. */
  private val Threads = 4

  /**
   * A ZIO runtime all of whose fibers, forked ones included, run on threads
   * with [[SmallStackBytes]] of stack.
   *
   * Its executor and its blocking executor are the same small-stack pool, so a
   * shift to the blocking executor cannot move work onto a thread with a larger
   * stack. A fatal error, whether ZIO reports it or it escapes to a pool thread,
   * completes `outcome` with the error rather than exiting the JVM; the fiber
   * that hit it never completes, so `outcome` is the only way to learn of it.
   */
  private final class SmallStackRuntime {
    private val threadIds = new AtomicInteger()

    /**
     * Creates daemon threads with a fixed small stack, so an abandoned run
     * cannot keep the JVM alive, and routes anything that escapes a thread to
     * `outcome`.
     */
    private val threadFactory: ThreadFactory = runnable => {
      val thread =
        new Thread(null, runnable, s"stack-safety-${threadIds.incrementAndGet()}", SmallStackBytes)
      thread.setDaemon(true)
      thread.setUncaughtExceptionHandler((_, t) => fail(t))
      thread
    }

    private val pool =
      new ThreadPoolExecutor(
        Threads,
        Threads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable](),
        threadFactory
      )

    private val executor = Executor.fromThreadPoolExecutor(pool)

    /** The run's exit, or the fatal error that stopped it. */
    val outcome = new CompletableFuture[Either[Throwable, Exit[Any, Any]]]()

    private def fail(t: Throwable): Unit = {
      outcome.complete(Left(t))
      ()
    }

    private val runtime = Unsafe.unsafe { implicit u =>
      Runtime.unsafe.fromLayer(
        Runtime.setExecutor(executor) ++
          Runtime.setBlockingExecutor(executor) ++
          Runtime.setReportFatal { t =>
            fail(t)
            throw t
          }
      )
    }

    /**
     * Starts `zio` on this runtime. `onExecutor` shifts the root fiber onto the
     * pool before it runs anything, and every fiber it forks inherits that
     * executor.
     */
    def start(zio: ZIO[Any, Any, Any]): Unit =
      Unsafe.unsafe { implicit u =>
        runtime.unsafe.fork(zio.onExecutor(executor)).unsafe.addObserver { exit =>
          outcome.complete(Right(exit))
          ()
        }
      }

    /** Releases the pool, abandoning any fiber left behind by a fatal error. */
    def shutdown(): Unit = {
      Unsafe.unsafe(implicit u => runtime.unsafe.shutdown())
      pool.shutdownNow()
      ()
    }
  }

  /**
   * Runs `zio` to completion on a fresh [[SmallStackRuntime]], returning its
   * exit, or the fatal error that stopped it, such as a `StackOverflowError`
   * on any of its fibers.
   */
  private def runOnSmallStacks(zio: ZIO[Any, Any, Any]): Task[Either[Throwable, Exit[Any, Any]]] =
    ZIO.acquireReleaseWith(ZIO.succeed(new SmallStackRuntime))(r => ZIO.succeed(r.shutdown())) { r =>
      ZIO.succeed(r.start(zio)) *> ZIO.fromCompletionStage(r.outcome)
    }

  def spec =
    suite("stack safety")(
      test("a single large chunk with a synchronous f does not overflow") {
        // One chunk, so this is a single round: a recursive loop would be as
        // deep as the whole 200k rather than anything `MaxClaimSize` bounds.
        val chunk = Chunk.fromArray(Array.fill(200000)(1))
        for {
          outcome <- runOnSmallStacks(ZStream.fromChunks(chunk).runForeachPar(4)(_ => Exit.unit))
        } yield assertTrue(outcome == Right(Exit.unit))
      },
      test("many chunks with a synchronous f do not overflow") {
        // Fusion makes rounds much longer than any one chunk, so a stream of
        // modest chunks reaches the same depth by a different route. How long
        // a round gets depends on how far the producer is ahead, bounded by
        // `bufferSize` chunks; a 64-chunk buffer allows rounds of 32,000
        // elements rather than the default 16 chunks' 8,000, so a recursive
        // loop overflows here without relying on a lucky schedule.
        val chunks = (0 until 400).map(i => Chunk.fromArray(Array.fill(500)(i)))
        for {
          outcome <- runOnSmallStacks(ZStream.fromChunks(chunks: _*).runForeachPar(4, 64)(_ => Exit.unit))
        } yield assertTrue(outcome == Right(Exit.unit))
      },
      test("a synchronous f that fails partway does not overflow") {
        // The failure path routes through `onError` rather than the success
        // continuation; it must be equally stack safe.
        val chunk = Chunk.fromArray(Array.tabulate(200000)(identity))
        for {
          outcome <- runOnSmallStacks(
            ZStream
              .fromChunks(chunk)
              .runForeachPar(4)(i => if (i == 150000) Exit.fail("boom") else Exit.unit)
              .either
          )
        } yield assertTrue(outcome == Right(Exit.succeed(Left("boom"))))
      },
      test("a synchronous f runs every element exactly once") {
        // `loop` hands control back to the interpreter every `YieldEvery`
        // elements and then resumes the same round; it must not skip or repeat
        // an element at that boundary. `f` returns an `Exit` so that it takes
        // the inline path the boundary interrupts.
        val size = 20000
        val runs = new AtomicIntegerArray(size)
        val chunk = Chunk.fromArray(Array.tabulate(size)(identity))
        for {
          outcome <- runOnSmallStacks(ZStream.fromChunks(chunk).runForeachPar(8) { i =>
            runs.incrementAndGet(i)
            Exit.unit
          })
          // Computed outside `assertTrue`, which would otherwise trace every
          // element and render a failure as one nested node per element.
          miscounted = (0 until size).filter(runs.get(_) != 1)
        } yield assertTrue(outcome == Right(Exit.unit), miscounted.isEmpty)
      }
    ) @@ TestAspect.timeout(120.seconds)
}
