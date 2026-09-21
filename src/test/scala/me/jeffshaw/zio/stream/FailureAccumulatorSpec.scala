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
import zio.test._

/**
 * Pins the failure protocol directly, as calls on one accumulator.
 *
 * `RunForeachParSpec` covers the same behavior end to end, through a real run
 * with racing workers; those tests stay. What they cannot show in isolation is
 * the case the protocol turns on: a run that failed while recording no cause at
 * all. That is asserted here.
 */
object FailureAccumulatorSpec extends ZIOSpecDefault {

  private def causeOf(exit: Exit[String, Unit]): Option[Cause[String]] =
    exit match {
      case Exit.Failure(cause) => Some(cause)
      case _                   => None
    }

  def spec =
    suite("FailureAccumulator")(
      test("a run that never fails succeeds with no cause") {
        for {
          acc    <- FailureAccumulator.make[String]
          result <- acc.result
        } yield assertTrue(result.isSuccess)
      },
      test("a recorded failure is returned as that cause") {
        for {
          acc    <- FailureAccumulator.make[String]
          _      <- acc.record(Cause.fail("boom"))
          result <- acc.result
        } yield assertTrue(causeOf(result).exists(_.failures == List("boom")))
      },
      test("two recorded failures accumulate rather than replace") {
        // The type cannot enforce `&&` over `=`; this is what guards it.
        for {
          acc    <- FailureAccumulator.make[String]
          _      <- acc.record(Cause.fail("first"))
          _      <- acc.record(Cause.fail("second"))
          result <- acc.result
        } yield assertTrue(causeOf(result).exists(_.failures.toSet == Set("first", "second")))
      },
      test("an interruption-only cause fails the run with an empty cause") {
        // The subtlety the whole type exists for: the signal fires, so the run
        // must fail, but nothing is recorded, so the cause is empty. Reading the
        // cause alone would call this a success.
        for {
          acc    <- FailureAccumulator.make[String]
          fiber  <- ZIO.fiberId
          _      <- acc.record(Cause.interrupt(fiber))
          result <- acc.result
        } yield assertTrue(!result.isSuccess) &&
          assertTrue(causeOf(result).exists(_.isEmpty))
      },
      test("an interruption-only cause still fires the signal") {
        for {
          acc   <- FailureAccumulator.make[String]
          fiber <- ZIO.fiberId
          _     <- acc.record(Cause.interrupt(fiber))
          // `await` must already be complete, so a timeout here means the signal
          // never fired and a real run would hang instead of failing fast.
          awaited <- acc.await.timeout(5.seconds)
        } yield assertTrue(awaited.isDefined)
      },
      test("a real failure alongside interrupts keeps the real cause") {
        // Only an interruption-*only* cause is skipped. A cause that carries a
        // genuine failure as well must still be recorded, or fail-fast would
        // discard the reason the run stopped.
        for {
          acc    <- FailureAccumulator.make[String]
          fiber  <- ZIO.fiberId
          _      <- acc.record(Cause.fail("real") && Cause.interrupt(fiber))
          result <- acc.result
        } yield assertTrue(causeOf(result).exists(_.failures == List("real")))
      },
      test("recording is sequenced before the signal fires") {
        // Fail-fast interrupts the other workers only after `await` completes,
        // so whichever cause fired the signal must already be readable by then.
        for {
          acc    <- FailureAccumulator.make[String]
          _      <- acc.record(Cause.fail("boom"))
          _      <- acc.await
          result <- acc.result
        } yield assertTrue(causeOf(result).exists(_.failures == List("boom")))
      }
    )
}
