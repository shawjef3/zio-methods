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
import zio.stream.ZStream
import zio.test._
import zio.test.TestAspect.nonFlaky

/**
 * Differential tests: `runForeachPar` against the combinator it is documented as
 * a faster alternative to, `mapZIOParUnordered` followed by `runDrain`.
 *
 * Every other spec asserts against a hand-written model of correct behavior,
 * which only catches deviations that were anticipated. These tests instead
 * compare against a reference implementation, so a divergence shows up whether
 * or not anyone predicted it. That is how the duplicated upstream-failure cause
 * was found: `runForeachPar` recorded a stream failure once per worker while the
 * base combinator recorded it once.
 *
 * ==What is compared==
 *
 * The documented contract, not incidental structure:
 *
 *   - whether the run succeeded or failed;
 *   - the failure values and defect messages, counted by occurrence, so that
 *     reporting one logical failure several times is caught;
 *   - whether the cause is interruption-only, and whether it is empty;
 *   - which elements `f` was applied to, as a multiset — but only on the
 *     success path, for the reason below.
 *
 * ==Why `visited` is compared only on success==
 *
 * Under failure neither combinator has a deterministic set of visited elements.
 * A worker can claim an element and be interrupted before the callback runs:
 * here the claim (`cursor.getAndIncrement()`) and `f` are separate effect
 * nodes, and in `ZChannel#mapOutZIOParUnordered` the element is handed to a
 * forked fiber that can be interrupted between the latch and `f`. Either way
 * fail-fast teardown can strand an already-claimed element, leaving a gap.
 *
 * So on the failure path this compares everything except `visited` exactly, and
 * holds `visited` to the invariants that *are* guaranteed: no element visited
 * twice, and no element visited that the stream never emitted. Comparing the
 * two subsets for equality would not test parity — it would test whether two
 * independent runs lost the same race, which base against itself would also
 * fail.
 *
 * Deliberately not compared: `Cause` tree shape and failure ordering. Those
 * differ run to run in both implementations — concurrent failures race — so
 * asserting on them would pin ZIO internals and produce flakiness rather than
 * signal.
 */
object DifferentialSpec extends ZIOSpecDefault {

  /**
   * The comparable summary of one run: what `f` saw, and how the effect ended.
   */
  private final case class Outcome(
    visited: Map[Int, Int],
    succeeded: Boolean,
    failures: Map[String, Int],
    defects: Map[String, Int],
    interruptedOnly: Boolean,
    causeEmpty: Boolean
  )

  private def tally(xs: List[String]): Map[String, Int] =
    xs.groupBy(identity).map { case (k, vs) => (k, vs.length) }

  private def summarize(visited: Map[Int, Int], exit: Exit[String, Any]): Outcome = {
    val cause = exit.causeOption
    Outcome(
      visited = visited,
      succeeded = exit.isSuccess,
      // Counted, not de-duplicated: one logical failure appearing n times is
      // exactly the bug this suite exists to catch, and a `Set` would hide it.
      failures = tally(cause.toList.flatMap(_.failures)),
      defects = tally(cause.toList.flatMap(_.defects).map(_.getMessage)),
      interruptedOnly = cause.exists(_.isInterruptedOnly),
      // Distinguishes "failed with no recorded cause" — what both produce when
      // every worker was merely interrupted — from a cause carrying interrupts.
      causeEmpty = cause.exists(_.isEmpty)
    )
  }

  /**
   * Runs `scenario` under both implementations and asserts the outcomes agree.
   *
   * `scenario` receives a `record` callback to wrap around whatever `f` it
   * builds, and a `run` that applies the combinator under test at the requested
   * parallelism. Each side gets a fresh `Ref`, so the two runs never share state.
   */
  /**
   * Drops occurrence counts, keeping only which failures and defects appeared.
   *
   * Needed when more than one invocation of `f` can fail concurrently: how many
   * failures are recorded before fail-fast interruption lands is a race in
   * *both* implementations, so the counts legitimately differ run to run. Cases
   * with a single logical failure keep the counts, since that is where
   * over-reporting would show up.
   */
  private def dropCounts(o: Outcome): Outcome =
    o.copy(
      failures = o.failures.map { case (k, _) => (k, 1) },
      defects = o.defects.map { case (k, _) => (k, 1) }
    )

  private def equivalent(
    n: Int,
    bufferSize: Int,
    countFailures: Boolean = true,
    elements: Set[Int] = Set.empty
  )(
    stream: ZStream[Any, String, Int],
    f: (Int, Int => UIO[Unit]) => ZIO[Any, String, Any]
  ): UIO[TestResult] = {
    def runOne(useOurs: Boolean): UIO[Outcome] =
      for {
        seen  <- Ref.make(Map.empty[Int, Int])
        record = (a: Int) => seen.update(m => m.updated(a, m.getOrElse(a, 0) + 1))
        body   = (a: Int) => f(a, record)
        exit <-
          (if (useOurs) stream.runForeachPar(n, bufferSize)(body)
           else stream.mapZIOParUnordered(n, bufferSize)(body).runDrain).exit
        visited <- seen.get
      } yield summarize(visited, exit)

    for {
      base <- runOne(useOurs = false)
      ours <- runOne(useOurs = true)
      normalize = (o: Outcome) => if (countFailures) o else dropCounts(o)
    } yield
      if (base.succeeded && ours.succeeded)
        // No interruption on the success path — `workerFiber.join` waits for
        // every worker — so `visited` is a sound observable and the two runs
        // must agree on it exactly.
        assertTrue(normalize(ours) == normalize(base))
      else {
        // Under fail-fast, an element can be claimed by a worker that is then
        // interrupted before its callback runs, so *which* elements were
        // recorded is scheduling-dependent in both implementations. Comparing
        // the two subsets would test whether two independent runs lost the same
        // race, which base-vs-base would also fail. Compare everything else
        // exactly, and hold `visited` to the invariants that are actually
        // guaranteed.
        val strip = (o: Outcome) => normalize(o).copy(visited = Map.empty)
        val visitedOk = (o: Outcome) =>
          o.visited.values.forall(_ == 1) &&
            (elements.isEmpty || o.visited.keySet.subsetOf(elements))
        assertTrue(strip(ours) == strip(base)) &&
        assertTrue(visitedOk(ours)) &&
        assertTrue(visitedOk(base))
      }
  }

  /** Parallelism settings worth exercising: serial, small, and above chunk size. */
  private val parallelisms = Chunk(1, 2, 8, 64)

  /** `f` that merely records the element. */
  private val justRecord: (Int, Int => UIO[Unit]) => ZIO[Any, String, Any] =
    (a, record) => record(a)

  def spec =
    suite("runForeachPar vs mapZIOParUnordered + runDrain")(
      test("clean stream, several chunk sizes") {
        checkAll(Gen.fromIterable(parallelisms) <*> Gen.fromIterable(Chunk(1, 8, 100))) { case (n, chunkSize) =>
          equivalent(n, 16)(ZStream.range(0, 200, chunkSize), justRecord)
        }
      } @@ nonFlaky(10),
      test("clean stream, several buffer sizes") {
        checkAll(Gen.fromIterable(parallelisms) <*> Gen.fromIterable(Chunk(1, 2, 128))) { case (n, bufferSize) =>
          equivalent(n, bufferSize)(ZStream.range(0, 200, 8), justRecord)
        }
      } @@ nonFlaky(10),
      test("empty stream") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.empty, justRecord)
        }
      } @@ nonFlaky(10),
      test("single chunk") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.fromChunk(Chunk.fromIterable(0 until 64)), justRecord)
        }
      } @@ nonFlaky(10),
      test("many small chunks") {
        val chunks = Chunk.fromIterable((0 until 200).map(i => Chunk(i, i + 1000)))
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.fromChunks(chunks.toSeq: _*), justRecord)
        }
      } @@ nonFlaky(10),
      test("stream fails after some elements") {
        // The case that exposed the duplicated-cause bug: one upstream failure
        // must surface identically however many workers observe it.
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16, elements = (0 until 64).toSet)(
            ZStream.range(0, 64, 8) ++ ZStream.fail("s-boom"),
            justRecord
          )
        }
      } @@ nonFlaky(10),
      test("stream fails immediately") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.fail("s-boom"), justRecord)
        }
      } @@ nonFlaky(10),
      test("stream dies after some elements") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16, elements = (0 until 64).toSet)(
            ZStream.range(0, 64, 8) ++ ZStream.die(new RuntimeException("s-die")),
            justRecord
          )
        }
      } @@ nonFlaky(10),
      test("callback fails on one element") {
        // Only the failing element's outcome is deterministic: with n > 1 the
        // set of elements visited before interruption lands is a race, so this
        // scenario fails fast on a stream small enough that both implementations
        // visit everything.
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(
            ZStream.fromChunk(Chunk(1)),
            (a, record) => record(a) *> ZIO.fail(s"boom-$a")
          )
        }
      } @@ nonFlaky(10),
      test("callback dies on one element") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(
            ZStream.fromChunk(Chunk(1)),
            (a, record) => record(a) *> ZIO.die(new RuntimeException(s"die-$a"))
          )
        }
      } @@ nonFlaky(10),
      test("callback fails on every element") {
        // Several workers fail concurrently here, so occurrence counts are a
        // race in both implementations; compare which failures appeared, not
        // how many times.
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16, countFailures = false)(
            ZStream.fromChunk(Chunk.fromIterable(0 until 8)),
            (_, _) => ZIO.fail("boom")
          )
        }
      } @@ nonFlaky(10),
      test("callback interrupts itself") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.range(0, 8, 2), (_, _) => ZIO.interrupt)
        }
      } @@ nonFlaky(10),
      test("callback yields, forcing interleaving") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(
            ZStream.range(0, 128, 4),
            (a, record) => ZIO.yieldNow *> record(a) *> ZIO.yieldNow
          )
        }
      } @@ nonFlaky(10),
      test("single-element stream") {
        checkAll(Gen.fromIterable(parallelisms)) { n =>
          equivalent(n, 16)(ZStream.fromChunk(Chunk(42)), justRecord)
        }
      } @@ nonFlaky(10)
      // Same rationale as the other specs: a broken handoff manifests as workers
      // that never terminate, which would hang rather than fail. Applies per
      // test, not to the suite as a whole.
    ) @@ TestAspect.timeout(30.seconds)
}
