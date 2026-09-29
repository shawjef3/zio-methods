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
import zio.stream.Take
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky

/**
 * Tests [[Dispatcher]] directly, driving it with scripted `fetch`
 * sequences that `runForeachPar` never produces.
 *
 * `runForeachPar` always supplies a well-behaved queue-backed `fetch` and wraps
 * the run in a scope it interrupts on failure. That masks two things this spec
 * asserts instead: how many times `onError` fires (the caller only ever sees the
 * combined cause), and whether every worker genuinely converges on a terminal
 * round (a stuck worker would be interrupted by the scope rather than hang).
 * Here `run` is used bare, so convergence is exactly "the returned effect
 * completes".
 */
object DispatcherSpec extends ZIOSpecDefault {

  /**
   * A `fetch` that yields `takes` in order and then repeats the final element
   * forever, counting invocations.
   *
   * The repeat matters: a terminal round is absorbing, so a correct run stops
   * pulling once it sees one. Repeating rather than failing on over-pull lets a
   * test distinguish "stopped pulling" (the count settles) from "kept pulling"
   * without the harness itself deciding what over-pulling means.
   */
  private def countingScriptedFetch[E, A](
    takes: Chunk[Take[E, A]]
  ): UIO[(ZIO[Any, Nothing, Take[E, A]], UIO[Int])] =
    Ref.make(0).map { calls =>
      val fetch =
        calls.getAndUpdate(_ + 1).map(i => takes(i min (takes.length - 1)))
      (fetch, calls.get)
    }

  /**
   * [[countingScriptedFetch]] with the call count discarded, for the tests that only
   * need a fetch to drive. Keeps the two-line tuple destructuring to the tests
   * that actually read the count, where it says something.
   */
  private def scripted[E, A](takes: Chunk[Take[E, A]]): UIO[ZIO[Any, Nothing, Take[E, A]]] =
    countingScriptedFetch[E, A](takes).map(_._1)

  private val noError: Cause[Any] => UIO[Unit] = _ => ZIO.unit

  /**
   * Runs the dispatcher with this spec's fixed type arguments.
   *
   * Every test here drives `[Any, String, Int]`, so spelling it out at
   * each call site buries the three things that actually vary: `n`, the
   * callback, and what the test does with a cause.
   */
  private def runWith(n: Int, fetch: ZIO[Any, Nothing, Take[String, Int]])(
    f: Int => IO[String, Any],
    onError: Cause[String] => UIO[Unit] = noError
  ): UIO[Unit] =
    Dispatcher.run[Any, String, Int](n, fetch, f, onError)

  def spec =
    suite("Dispatcher")(
      test("every worker converges when the first fetch is terminal") {
        // The seed round is already exhausted, so the very first fetch returns
        // end-of-stream and no data round is ever published. Every worker must
        // still terminate: the elected fetcher via its own branch, the rest by
        // awaiting the terminal round the fetcher publishes.
        checkAll(Gen.fromIterable(Chunk(1, 2, 8, 64))) { n =>
          for {
            visited <- Ref.make(0)
            fetch <- scripted[String, Int](Chunk(Take.end))
            _ <- runWith(n, fetch)(_ => visited.update(_ + 1))
            res <- visited.get
          } yield assertTrue(res == 0)
        }
      } @@ nonFlaky(20),
      test("every worker converges after data rounds") {
        // n far exceeds the elements available, so most workers only await rounds.
        val chunks = Chunk(Chunk(1, 2, 3), Chunk(4, 5), Chunk(6))
        val script = chunks.map(Take.chunk) :+ Take.end
        for {
          visited <- Ref.make(Vector.empty[Int])
          fetch <- scripted[String, Int](script)
          _ <- runWith(64, fetch)(a => visited.update(_ :+ a))
          res <- visited.get
        } yield assertTrue(res.sorted == Vector(1, 2, 3, 4, 5, 6))
      } @@ nonFlaky(50),
      test("fetch is invoked exactly once per round") {
        // With one chunk plus a terminal, a correct run pulls exactly twice
        // regardless of n. A double election pulls more, and so does starting
        // every worker from one pre-built `loop(seed)`: see "Starting the
        // workers" on `Dispatcher.run`.
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to 100)), Take.end)
        checkAll(Gen.fromIterable(Chunk(2, 16, 128))) { n =>
          for {
            fetchAndCount <- countingScriptedFetch[String, Int](script)
            (fetch, count) = fetchAndCount
            _ <- runWith(n, fetch)(_ => ZIO.unit)
            calls <- count
          } yield assertTrue(calls == script.length)
        }
      } @@ nonFlaky(50),
      test("empty chunks mid-stream re-elect a fetcher without stalling") {
        // A zero-length chunk makes `i == 0 == length` fire immediately, so the
        // round is published and instantly re-elects a fetcher. Several in a row
        // must not stall the run or drop the surrounding elements.
        val script =
          Chunk(
            Take.chunk(Chunk(1, 2)),
            Take.chunk(Chunk.empty[Int]),
            Take.chunk(Chunk.empty[Int]),
            Take.chunk(Chunk(3)),
            Take.end
          )
        for {
          visited <- Ref.make(Vector.empty[Int])
          fetchAndCount <- countingScriptedFetch[String, Int](script)
          (fetch, count) = fetchAndCount
          _ <- runWith(16, fetch)(a => visited.update(_ :+ a))
          res <- visited.get
          calls <- count
        } yield assertTrue(res.sorted == Vector(1, 2, 3)) && assertTrue(calls == script.length)
      } @@ nonFlaky(50),
      test("a clean end-of-stream never invokes onError") {
        val script = Chunk(Take.chunk(Chunk(1, 2, 3)), Take.end)
        for {
          errors <- Ref.make(0)
          fetch <- scripted[String, Int](script)
          _ <- runWith(16, fetch)(_ => ZIO.unit, _ => errors.update(_ + 1))
          res <- errors.get
        } yield assertTrue(res == 0)
      } @@ nonFlaky(50),
      test("a failure terminal is reported exactly once, whatever n is") {
        val script = Chunk(Take.chunk(Chunk(1, 2, 3)), Take.fail("boom"))
        checkAll(Gen.fromIterable(Chunk(1, 2, 8, 64))) { n =>
          for {
            causes <- Ref.make(Vector.empty[Cause[String]])
            fetch <- scripted[String, Int](script)
            _ <- runWith(n, fetch)(_ => ZIO.unit, c => causes.update(_ :+ c))
            res <- causes.get
          } yield assertTrue(res.length == 1) &&
            assertTrue(res.forall(_.failures == List("boom")))
        }
      } @@ nonFlaky(50),
      test("a callback failure is reported and the run still converges") {
        // Interrupting the other workers is the caller's job, so here the run
        // must still complete rather than hang.
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to 32)), Take.end)
        for {
          causes <- Ref.make(Vector.empty[Cause[String]])
          fetch <- scripted[String, Int](script)
          _ <- runWith(4, fetch)(
            a => ZIO.fail(s"odd-$a").when(a % 2 == 0),
            c => causes.update(_ :+ c)
          )
          res <- causes.get
        } yield assertTrue(res.nonEmpty) && assertTrue(res.forall(_.failures.nonEmpty))
      } @@ nonFlaky(50),
      test("a callback defect is reported, not swallowed") {
        val boom = new RuntimeException("die")
        val script = Chunk(Take.chunk(Chunk(1)), Take.end)
        for {
          causes <- Ref.make(Vector.empty[Cause[String]])
          fetch <- scripted[String, Int](script)
          _ <- runWith(4, fetch)(_ => ZIO.die(boom), c => causes.update(_ :+ c))
          res <- causes.get
        } yield assertTrue(res.exists(_.defects == List(boom)))
      } @@ nonFlaky(50),
      test("a single chunk keeps all n workers busy at once") {
        // Each element blocks until every worker has arrived, so the run
        // completes only if all n run together.
        val n = 16
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to n)), Take.end)
        for {
          arrived <- Ref.make(0)
          allArrived <- Promise.make[Nothing, Unit]
          fetch <- scripted[String, Int](script)
          _ <- runWith(n, fetch)(_ =>
            arrived.updateAndGet(_ + 1).flatMap { count =>
              allArrived.succeed(()).when(count == n) *> allArrived.await
            }
          )
        } yield assertCompletes
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("a chunk large enough to batch claims still keeps all n workers busy") {
        // The test above has `length == n`, so its claim size is 1. This chunk is
        // large enough for batched claims, which must still reach every worker.
        val n = 16
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to (n * 64))), Take.end)
        for {
          arrived <- Ref.make(0)
          allArrived <- Promise.make[Nothing, Unit]
          fetch <- scripted[String, Int](script)
          _ <- runWith(n, fetch)(_ =>
            arrived.updateAndGet(_ + 1).flatMap { count =>
              // Only the first n elements gate on each other; the rest run
              // freely, so the run can finish once saturation is shown.
              allArrived.succeed(()).when(count == n) *> allArrived.await
            }
          )
        } yield assertCompletes
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("claims partition the chunk at every length/n ratio") {
        // Different length/n ratios give different claim sizes, including ones where
        // the final claim is short. It also guards batched election: electing by
        // `i == length` on a claim size above 1 elects nobody, which times out here.
        checkAll(
          Gen.fromIterable(
            for {
              length <- Chunk(1, 7, 63, 64, 65, 1000, 1023)
              n <- Chunk(1, 2, 3, 16, 64)
            } yield (length, n)
          )
        ) { case (length, n) =>
          val script = Chunk(Take.chunk(Chunk.fromIterable(1 to length)), Take.end)
          for {
            counts <- Ref.make(Map.empty[Int, Int])
            fetchAndCount <- countingScriptedFetch[String, Int](script)
            (fetch, calls) = fetchAndCount
            _ <- runWith(n, fetch)(a => Visits.record(counts, a))
            res <- counts.get
            fetches <- calls
          } yield assertTrue(res.size == length) &&
            assertTrue(res.values.forall(_ == 1)) &&
            // Exactly one fetcher per round, so one pull for the chunk and one
            // for the terminal: never zero (a lost election hangs) and never two.
            assertTrue(fetches == 2)
        }
      } @@ nonFlaky(20),
      test("n = 1 visits every element") {
        // A single worker is not forked: `WorkerPool.replicate` runs it on the
        // calling fiber, so this is a separate start path from `n >= 2`.
        val chunks = Chunk(Chunk(1, 2, 3), Chunk(4, 5))
        val script = chunks.map(Take.chunk) :+ Take.end
        for {
          visited <- Ref.make(Vector.empty[Int])
          fetch <- scripted[String, Int](script)
          _ <- runWith(1, fetch)(a => visited.update(_ :+ a))
          res <- visited.get
        } yield assertTrue(res == Vector(1, 2, 3, 4, 5))
      } @@ nonFlaky(20),
      test("every element is claimed exactly once across many rounds") {
        val chunks = Chunk.fromIterable((0 until 200).map(i => Chunk(i, i + 1000)))
        val script = chunks.map(Take.chunk) :+ Take.end
        val total = chunks.map(_.length).sum
        for {
          counts <- Ref.make(Map.empty[Int, Int])
          fetch <- scripted[String, Int](script)
          _ <- runWith(32, fetch)(a => Visits.record(counts, a))
          res <- counts.get
        } yield Visits.eachOnce(res, total)
      } @@ nonFlaky(50)
      // Per test: a broken round handoff hangs rather than fails.
    ) @@ TestAspect.timeout(5.seconds)
}
