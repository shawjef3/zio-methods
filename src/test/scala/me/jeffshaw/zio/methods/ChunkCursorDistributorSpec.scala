/*
 * Copyright 2027 Jeffrey Shaw
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

package me.jeffshaw.zio.methods

import zio._
import zio.stream.Take
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky

/**
 * Tests [[ChunkCursorDistributor]] directly, driving it with scripted `fetch`
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
object ChunkCursorDistributorSpec extends ZIOSpecDefault {

  /**
   * A `fetch` that yields `takes` in order and then repeats the final element
   * forever, counting invocations.
   *
   * The repeat matters: a terminal round is absorbing, so a correct run stops
   * pulling once it sees one. Repeating rather than failing on over-pull lets a
   * test distinguish "stopped pulling" (the count settles) from "kept pulling"
   * without the harness itself deciding what over-pulling means.
   */
  private def scriptedFetch[E, A](
    takes: Chunk[Take[E, A]]
  ): UIO[(ZIO[Any, Nothing, Take[E, A]], UIO[Int])] =
    Ref.make(0).map { calls =>
      val fetch =
        calls.getAndUpdate(_ + 1).map(i => takes(i min (takes.length - 1)))
      (fetch, calls.get)
    }

  private val noError: Cause[Any] => UIO[Unit] = _ => ZIO.unit

  def spec =
    suite("ChunkCursorDistributor")(
      test("every worker converges when the first fetch is terminal") {
        // The seed round is already exhausted, so the very first fetch returns
        // end-of-stream and no data round is ever published. Every worker must
        // still terminate: the elected fetcher via its own branch, the rest by
        // awaiting the terminal round the fetcher publishes.
        checkAll(Gen.fromIterable(Chunk(1, 2, 8, 64))) { n =>
          for {
            visited        <- Ref.make(0)
            fetchAndCount  <- scriptedFetch[String, Int](Chunk(Take.end))
            (fetch, _)      = fetchAndCount
            _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                   n,
                   fetch,
                   _ => visited.update(_ + 1),
                   noError
                 )
            res <- visited.get
          } yield assertTrue(res == 0)
        }
      } @@ nonFlaky(20),
      test("every worker converges after data rounds") {
        // n far exceeds the elements available, so most workers spend the run
        // awaiting rounds rather than claiming elements. All of them must still
        // observe the terminal and stop.
        val chunks = Chunk(Chunk(1, 2, 3), Chunk(4, 5), Chunk(6))
        val script = chunks.map(Take.chunk) :+ Take.end
        for {
          visited       <- Ref.make(Vector.empty[Int])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 64,
                 fetch,
                 a => visited.update(_ :+ a),
                 noError
               )
          res <- visited.get
        } yield assertTrue(res.sorted == Vector(1, 2, 3, 4, 5, 6))
      } @@ nonFlaky(50),
      test("fetch is invoked exactly once per round") {
        // "Exactly one worker observes the boundary value, by construction."
        // With one chunk plus a terminal, a correct run pulls exactly twice
        // regardless of n; a double election would pull more.
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to 100)), Take.end)
        checkAll(Gen.fromIterable(Chunk(2, 16, 128))) { n =>
          for {
            fetchAndCount <- scriptedFetch[String, Int](script)
            (fetch, count) = fetchAndCount
            _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                   n,
                   fetch,
                   _ => ZIO.unit,
                   noError
                 )
            calls <- count
          } yield assertTrue(calls == script.length)
        }
      } @@ nonFlaky(50),
      test("stops pulling once a terminal round is reached") {
        // A terminal round is absorbing. The scripted fetch repeats `Take.end`
        // forever, so a worker that looped back into the fetcher branch after
        // termination would keep incrementing the count.
        val script = Chunk(Take.chunk(Chunk(1, 2, 3)), Take.end)
        for {
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, count) = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 32,
                 fetch,
                 _ => ZIO.unit,
                 noError
               )
          before <- count
          // Give any still-running worker a chance to pull again; the count
          // must not move after the run has completed.
          _     <- Live.live(ZIO.sleep(20.millis))
          after <- count
        } yield assertTrue(before == script.length) && assertTrue(after == before)
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
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
          visited       <- Ref.make(Vector.empty[Int])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, count) = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 16,
                 fetch,
                 a => visited.update(_ :+ a),
                 noError
               )
          res   <- visited.get
          calls <- count
        } yield assertTrue(res.sorted == Vector(1, 2, 3)) && assertTrue(calls == script.length)
      } @@ nonFlaky(50),
      test("a clean end-of-stream never invokes onError") {
        // `Cause.empty` doubles as the clean-EOS sentinel, and `stopOn` branches
        // on `terminalCause.isEmpty`. A clean end must not be reported as a
        // failure to any of the n workers.
        val script = Chunk(Take.chunk(Chunk(1, 2, 3)), Take.end)
        for {
          errors        <- Ref.make(0)
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 16,
                 fetch,
                 _ => ZIO.unit,
                 _ => errors.update(_ + 1)
               )
          res <- errors.get
        } yield assertTrue(res == 0)
      } @@ nonFlaky(50),
      test("a failure terminal is reported exactly once, whatever n is") {
        // The fetcher that pulls a failing terminal is its sole reporter; the
        // workers that later observe the terminal round do not re-report it. So
        // one upstream failure yields exactly one cause no matter how many
        // workers converge on it, matching the base combinator.
        val script = Chunk(Take.chunk(Chunk(1, 2, 3)), Take.fail("boom"))
        checkAll(Gen.fromIterable(Chunk(1, 2, 8, 64))) { n =>
          for {
            causes        <- Ref.make(Vector.empty[Cause[String]])
            fetchAndCount <- scriptedFetch[String, Int](script)
            (fetch, _)     = fetchAndCount
            _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                   n,
                   fetch,
                   _ => ZIO.unit,
                   c => causes.update(_ :+ c)
                 )
            res <- causes.get
          } yield assertTrue(res.length == 1) &&
            assertTrue(res.forall(_.failures == List("boom")))
        }
      } @@ nonFlaky(50),
      test("a callback failure is reported and the run still converges") {
        // `f` failing routes through `foldCauseZIO(onError, ...)`, which stops
        // that worker's loop. Interruption of the others is the caller's job, so
        // here the run must still complete rather than hang.
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to 32)), Take.end)
        for {
          causes        <- Ref.make(Vector.empty[Cause[String]])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 4,
                 fetch,
                 a => if (a % 2 == 0) ZIO.fail(s"odd-$a") else ZIO.unit,
                 c => causes.update(_ :+ c)
               )
          res <- causes.get
        } yield assertTrue(res.nonEmpty) && assertTrue(res.forall(_.failures.nonEmpty))
      } @@ nonFlaky(50),
      test("a callback defect is reported, not swallowed") {
        val boom   = new RuntimeException("die")
        val script = Chunk(Take.chunk(Chunk(1)), Take.end)
        for {
          causes        <- Ref.make(Vector.empty[Cause[String]])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 4,
                 fetch,
                 _ => ZIO.die(boom),
                 c => causes.update(_ :+ c)
               )
          res <- causes.get
        } yield assertTrue(res.exists(_.defects == List(boom)))
      } @@ nonFlaky(50),
      test("a single chunk keeps all n workers busy at once") {
        // The design claim the cursor exists to satisfy: one chunk of >= n
        // elements must saturate all n workers. Each element blocks until every
        // worker has arrived, so the run completes only if they run together.
        val n      = 16
        val script = Chunk(Take.chunk(Chunk.fromIterable(1 to n)), Take.end)
        for {
          arrived       <- Ref.make(0)
          allArrived    <- Promise.make[Nothing, Unit]
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 n,
                 fetch,
                 _ =>
                   arrived.updateAndGet(_ + 1).flatMap { count =>
                     allArrived.succeed(()).when(count == n) *> allArrived.await
                   },
                 noError
               )
        } yield assertCompletes
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("n = 1 visits every element") {
        // `runForeachPar` short-circuits n <= 1 to `runForeach`, so this path is
        // unreachable through the public combinator.
        val chunks = Chunk(Chunk(1, 2, 3), Chunk(4, 5))
        val script = chunks.map(Take.chunk) :+ Take.end
        for {
          visited       <- Ref.make(Vector.empty[Int])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 1,
                 fetch,
                 a => visited.update(_ :+ a),
                 noError
               )
          res <- visited.get
        } yield assertTrue(res == Vector(1, 2, 3, 4, 5))
      } @@ nonFlaky(20),
      test("every element is claimed exactly once across many rounds") {
        // The cursor's core guarantee, at the distributor level: no index is
        // handed out twice and none is skipped, across many small rounds where
        // fetcher election happens constantly.
        val chunks = Chunk.fromIterable((0 until 200).map(i => Chunk(i, i + 1000)))
        val script = chunks.map(Take.chunk) :+ Take.end
        val total  = chunks.map(_.length).sum
        for {
          counts        <- Ref.make(Map.empty[Int, Int])
          fetchAndCount <- scriptedFetch[String, Int](script)
          (fetch, _)     = fetchAndCount
          _ <- ChunkCursorDistributor.run[Any, String, String, Int](
                 32,
                 fetch,
                 a => counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1)),
                 noError
               )
          res <- counts.get
        } yield assertTrue(res.size == total) && assertTrue(res.values.forall(_ == 1))
      } @@ nonFlaky(50)
      // Same rationale as RunForeachParSpec: a broken round handoff manifests as
      // workers that never terminate, which would hang rather than fail. This
      // aspect applies per test, not to the suite as a whole.
    ) @@ TestAspect.timeout(5.seconds)
}
