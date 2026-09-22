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
import zio.stream._
import zio.test._

/**
 * Guards the dispatch loop against stack overflow when `f` does not suspend.
 *
 * `foldCauseZIO` on an already-completed `Exit` runs its continuation inline
 * rather than returning to the ZIO interpreter, so a synchronous `f` makes the
 * `loop`/`runClaim` cycle ordinary JVM recursion whose depth is the length of
 * the round rather than of a claim. `Round.MaxStride` bounds a claim and does
 * not bound this.
 *
 * These tests run on a thread with an explicitly small stack, because the
 * default `zio-test` fiber stack is generous enough to hide the bug: before the
 * `TrampolineEvery` fix, a 200,000-element chunk passed here on the default
 * stack and overflowed under JMH. Pinning the stack size is what makes the test
 * a reliable regression guard rather than an environment-dependent one.
 */
object StackSafetySpec extends ZIOSpecDefault {

  private val SmallStackBytes = 512L * 1024

  /**
   * Runs `zio` to completion on a thread with `SmallStackBytes` of stack,
   * returning the throwable that escaped, if any. A `StackOverflowError` here
   * surfaces as a fiber defect, so it is collected rather than thrown.
   */
  private def runOnSmallStack[E](zio: ZIO[Any, E, Any]): UIO[Option[Throwable]] =
    ZIO.succeed {
      @volatile var escaped: Option[Throwable] = None
      val t = new Thread(
        null,
        () =>
          escaped =
            try {
              Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio).getOrThrowFiberFailure())
              None
            } catch { case t: Throwable => Some(t) },
        "stack-safety-probe",
        SmallStackBytes
      )
      t.start()
      t.join()
      escaped
    }

  /** Collects every stack overflow reachable from a cause, at any nesting. */
  private def isStackOverflow(t: Throwable): Boolean =
    t.isInstanceOf[StackOverflowError] ||
      Option(t.getCause).exists(isStackOverflow) ||
      t.getSuppressed.exists(isStackOverflow) ||
      Option(t.getMessage).exists(_.contains("StackOverflowError"))

  def spec =
    suite("stack safety")(
      test("a single large chunk with a synchronous f does not overflow") {
        // One chunk, so this is a single round: the recursion depth is the whole
        // 200k rather than anything `MaxStride` bounds. This is the case that
        // failed before the fix.
        val chunk = Chunk.fromArray(Array.fill(200000)(1))
        for {
          escaped <- runOnSmallStack(ZStream.fromChunks(chunk).runForeachPar(4)(_ => Exit.unit))
        } yield assertTrue(!escaped.exists(isStackOverflow))
      },
      test("many chunks with a synchronous f do not overflow") {
        // Fusion makes rounds much longer than any one chunk, so a stream of
        // modest chunks reaches the same depth by a different route.
        val chunks = (0 until 400).map(i => Chunk.fromArray(Array.fill(500)(i)))
        for {
          escaped <- runOnSmallStack(ZStream.fromChunks(chunks: _*).runForeachPar(4)(_ => Exit.unit))
        } yield assertTrue(!escaped.exists(isStackOverflow))
      },
      test("a synchronous f that fails partway does not overflow") {
        // The failure path routes through `onError` rather than the success
        // continuation; it must be equally stack safe.
        val chunk = Chunk.fromArray(Array.tabulate(200000)(identity))
        for {
          escaped <- runOnSmallStack(
            ZStream
              .fromChunks(chunk)
              .runForeachPar(4)(i => if (i == 150000) ZIO.fail("boom") else Exit.unit)
              .either
          )
        } yield assertTrue(!escaped.exists(isStackOverflow))
      },
      test("every element still runs exactly once across the trampoline") {
        // The trampoline resets a counter and re-enters `loop`; it must not skip
        // or repeat an element at the boundary.
        val size = 20000
        val chunk = Chunk.fromArray(Array.tabulate(size)(identity))
        for {
          seen <- Ref.make(Set.empty[Int])
          _ <- ZStream.fromChunks(chunk).runForeachPar(8)(i => seen.update(_ + i))
          s <- seen.get
        } yield assertTrue(s.size == size)
      }
    ) @@ TestAspect.timeout(120.seconds)
}
