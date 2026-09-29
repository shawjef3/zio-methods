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
import zio.stream.{Take, ZStream}
import zio.test._

import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Guards against retention that correctness tests cannot see: every element is
 * still visited exactly once, and the only symptom is an OOM on a long stream.
 * Rounds link forward (see [[Round]]), so holding an early round, a drained
 * round's chunk, or the seed all make retention grow with the stream rather
 * than stay bounded by `bufferSize`.
 *
 * These tests measure reachability directly with weak references. The payload
 * tests hold the run open mid-flight by blocking one callback on the last
 * element, so the worker pool is live and the round chain fully built at the
 * moment of measurement. `runForeach` and `mapZIOParUnordered` serve as
 * controls: both retain only the element they are blocked on. The round test
 * measures from inside the final fetch, for the same reason.
 */
object RetentionSpec extends ZIOSpecDefault {

  /**
   * Follows the round chain from the seed as a run advances. It holds a strong
   * reference only to the newest round it has reached, which pins nothing
   * earlier because the links point forward, and a weak one to every
   * `SampleEvery`-th round it passes. A sampled round that is still reachable
   * once it is behind the newest is therefore being held by the run.
   *
   * Only the fetcher touches it, and successive fetchers are ordered by the
   * round handoff; `@volatile` makes that independent of those details.
   */
  private final class RoundChaser {
    @volatile var newest: Round[Nothing, Int] = _
    @volatile var passed = 0
    @volatile var fetches = 0
    @volatile var sampled = 0
    @volatile var alive = -1
    private[this] val samples = new ConcurrentLinkedQueue[WeakReference[AnyRef]]

    // The effect-level `poll`, because `Promise#unsafe` is `private[zio]`.
    val advance: UIO[Unit] =
      ZIO.suspendSucceed(newest.next.poll).flatMap {
        case Some(done) =>
          done match {
            case s: Exit.Success[_] =>
              ZIO.succeed {
                newest = s.value.asInstanceOf[Round[Nothing, Int]]
                passed += 1
                if (passed % SampleEvery == 0) samples.add(new WeakReference[AnyRef](newest))
              } *> advance
            case _ => ZIO.unit
          }
        case None => ZIO.unit
      }

    /**
     * Counts the sampled rounds still reachable. A leak keeps all of them, and a
     * GC that happens not to run looks the same, so this retries a few times
     * before believing a high count.
     */
    def measure(): Unit = {
      var attempts = 0
      var live = Int.MaxValue
      while (live > RoundSlack && attempts < 5) {
        java.lang.System.gc()
        Thread.sleep(50)
        var count = 0
        samples.forEach(r => if (r.get() ne null) count += 1)
        live = count
        attempts += 1
      }
      sampled = samples.size
      alive = live
    }
  }

  private val RoundCount = 200000
  private val SampleEvery = 1000
  // The newest sample is the round being fetched from and is live by
  // definition; a worker still parked a round or two behind can hold one more.
  private val RoundSlack = 3

  /**
   * Runs a [[Dispatcher]] over `RoundCount` one-element rounds and, from inside
   * the final fetch, reports how many sampled rounds are still reachable.
   *
   * The dispatcher is built here rather than through `ChunkCursorDistributor`
   * only so the seed can be read before the workers start; `run` is the same
   * one production uses. The seed goes straight into the chaser, never into a
   * local that a closure could capture and keep alive. `pinSeed` holds it for
   * the whole run instead, which is the leak, as a control on the measurement.
   * `blockFirst` holds the first element's `f` open until after the
   * measurement, so one worker stays inside `f` while the others advance
   * through every remaining round.
   */
  private def roundsReachableDuringRun(n: Int, pinSeed: Boolean = false, blockFirst: Boolean = false): UIO[(Int, Int)] =
    ZIO.suspendSucceed {
      val chaser = new RoundChaser
      val pin = new java.util.concurrent.atomic.AtomicReference[AnyRef]
      // Opened from the final fetch, once the measurement has been taken.
      val gate = Promise.unsafe.make[Nothing, Unit](FiberId.None)(Unsafe)
      val fetch: UIO[Take[Nothing, Int]] =
        chaser.advance *> ZIO.suspendSucceed {
          chaser.fetches += 1
          if (chaser.fetches <= RoundCount) Exit.succeed(Take.single(chaser.fetches))
          else {
            chaser.measure()
            gate.succeed(()).as(Take.end)
          }
        }
      val f: Int => UIO[Any] = a => gate.await.when(blockFirst && a == 1)
      val dispatcher = new Dispatcher[Any, Nothing, Nothing, Int](n, fetch, f, _ => ZIO.unit)
      chaser.newest = dispatcher.seedForTesting
      if (pinSeed) pin.set(chaser.newest)
      dispatcher.run.as((chaser.sampled, chaser.alive)) <* ZIO.succeed(pin.set(null))
    }

  private final class Payload(val id: Int) {
    // Large enough that retaining the whole stream is an OOM rather than a
    // curiosity, and that the GC has an incentive to actually collect.
    val filler = new Array[Byte](1024)
  }

  private val total = 20000
  private val chunkSz = 100
  private val sampleOf = 100
  private val blockAt = total - 1

  private def source =
    ZStream.unfoldChunk(0) { i =>
      if (i >= total) None
      else
        Some(
          (Chunk.fromIterable((i until (i + chunkSz).min(total)).map(new Payload(_))), i + chunkSz)
        )
    }

  /**
   * Runs `consume` over the source, blocking on the last element, and reports
   * how many of the sampled payloads are still reachable at that point.
   */
  private def reachableDuringRun(
    consume: (Payload => ZIO[Any, Nothing, Any]) => ZIO[Any, Any, Any]
  ): ZIO[Any, Any, (Int, Int)] =
    for {
      refs <- Ref.make(List.empty[WeakReference[Payload]])
      blocked <- Promise.make[Nothing, Unit]
      release <- Promise.make[Nothing, Unit]
      f = (p: Payload) =>
        if (p.id == blockAt) blocked.succeed(()) *> release.await
        else if (p.id % sampleOf == 0) refs.update(new WeakReference(p) :: _)
        else ZIO.unit
      fiber <- consume(f).fork
      _ <- blocked.await
      // Let the remaining workers finish everything they can.
      _ <- ZIO.sleep(500.millis)
      _ <- ZIO.succeed { java.lang.System.gc(); Thread.sleep(300); java.lang.System.gc() }
      rs <- refs.get
      alive = rs.count(_.get() != null)
      _ <- release.succeed(()) *> fiber.interrupt
    } yield (rs.size, alive)

  /**
   * Counts the payloads still reachable, apart from those in `held`, retrying
   * the GC a few times because a single one can be skipped.
   */
  private def countReachable(refs: ConcurrentLinkedQueue[WeakReference[Payload]], held: Set[Int]): Int = {
    var live = 0
    var attempts = 0
    while (attempts < 4) {
      java.lang.System.gc()
      Thread.sleep(80)
      live = 0
      refs.forEach { r =>
        val p = r.get()
        if ((p ne null) && !held(p.id)) live += 1
      }
      attempts += 1
    }
    live
  }

  /**
   * Runs `consume` over the source with one callback hung on an element in the
   * middle of the stream for the whole run, and another blocked on the last
   * element so the run stays open. Reports how many payloads other than those
   * two are reachable once everything else has been processed.
   *
   * With `n = 64` the source fuses into rounds large enough to batch claims,
   * so the hung element sits inside a multi-element claim, and the rest of that
   * claim is what the hung callback's continuation has to keep.
   */
  private def reachableWithHungCallback(consume: (Payload => UIO[Any]) => ZIO[Any, Any, Any]): UIO[Int] = {
    val hangAt = total / 2
    for {
      refs <- ZIO.succeed(new ConcurrentLinkedQueue[WeakReference[Payload]])
      hung <- Promise.make[Nothing, Unit]
      last <- Promise.make[Nothing, Unit]
      release <- Promise.make[Nothing, Unit]
      f = (p: Payload) => {
        refs.add(new WeakReference(p))
        if (p.id == hangAt) hung.succeed(()) *> release.await
        else if (p.id == blockAt) last.succeed(()) *> release.await
        else ZIO.unit
      }
      fiber <- consume(f).fork
      _ <- hung.await *> last.await
      _ <- ZIO.sleep(300.millis)
      alive <- ZIO.succeed(countReachable(refs, Set(hangAt, blockAt)))
      _ <- release.succeed(()) *> fiber.interrupt
    } yield alive
  }

  private val idleAfter = 16

  /**
   * Runs `consume` over a stream that emits `idleAfter` chunks as fast as it
   * can and then emits nothing more, and reports how many payloads are
   * reachable once every element emitted has been processed.
   */
  private def reachableWhileIdle(consume: (ZStream[Any, Nothing, Payload], Payload => UIO[Any]) => ZIO[Any, Any, Any]) =
    for {
      refs <- ZIO.succeed(new ConcurrentLinkedQueue[WeakReference[Payload]])
      processed <- Ref.make(0)
      all <- Promise.make[Nothing, Unit]
      stream = ZStream.unfoldChunk(0) { k =>
        if (k >= idleAfter) None
        else Some((Chunk.fromIterable((k * chunkSz until (k + 1) * chunkSz).map(new Payload(_))), k + 1))
      } ++ ZStream.never
      f = (p: Payload) =>
        ZIO.succeed(refs.add(new WeakReference(p))) *>
          processed.updateAndGet(_ + 1).flatMap(c => all.succeed(()).when(c == idleAfter * chunkSz))
      fiber <- consume(stream, f).fork
      _ <- all.await
      _ <- ZIO.sleep(300.millis)
      alive <- ZIO.succeed(countReachable(refs, Set.empty))
      _ <- fiber.interrupt
    } yield alive

  def spec =
    suite("retention")(
      test("a drained chunk is not retained for the life of the run") {
        for {
          res <- reachableDuringRun(f => source.runForeachPar(64, 16)(f))
          (sampled, alive) = res
        } yield assertTrue(
          sampled > 100,
          // Only the element the run is blocked on may be held. A small slack
          // covers the chunk still being dispatched when we measured.
          alive <= chunkSz
        )
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds),
      test("retention does not grow with n") {
        for {
          one <- reachableDuringRun(f => source.runForeachPar(1, 16)(f))
          many <- reachableDuringRun(f => source.runForeachPar(512, 16)(f))
        } yield assertTrue(one._2 <= chunkSz, many._2 <= chunkSz)
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds),
      test("matches the retention of the combinators it replaces") {
        for {
          seq <- reachableDuringRun(f => source.runForeach(f))
          par <- reachableDuringRun(f => source.mapZIOParUnordered(64)(p => f(p)).runDrain)
          ours <- reachableDuringRun(f => source.runForeachPar(64, 16)(f))
          _ <- ZIO.succeed(
            println(
              s"[retention] runForeach=${seq._2} mapZIOParUnordered=${par._2} runForeachPar=${ours._2} (of ${ours._1} sampled)"
            )
          )
          // Not worse than the baselines by more than a chunk.
        } yield assertTrue(ours._2 <= seq._2 + chunkSz, ours._2 <= par._2 + chunkSz)
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds),
      test("a long run does not retain the rounds it has finished with") {
        // One-element rounds, so there is one round per element. If anything
        // holds the seed for the life of the run, every sampled round is still
        // reachable at the end rather than only the newest few. `n == 1` is
        // included because a single worker is not forked, so its start effect
        // is held by a different frame than the workers' at `n >= 2`.
        checkAll(Gen.fromIterable(Chunk(1, 2))) { n =>
          for {
            res <- roundsReachableDuringRun(n)
            (sampled, alive) = res
          } yield assertTrue(sampled == RoundCount / SampleEvery, alive <= RoundSlack)
        }
      } @@ TestAspect.timeout(120.seconds),
      test("a slow callback does not retain the rounds published while it runs") {
        // One worker stays inside `f` on the first element for the whole run
        // while the others advance through every round.
        checkAll(Gen.fromIterable(Chunk(2, 4))) { n =>
          for {
            res <- roundsReachableDuringRun(n, blockFirst = true)
            (sampled, alive) = res
          } yield assertTrue(sampled == RoundCount / SampleEvery, alive <= RoundSlack)
        }
      } @@ TestAspect.timeout(120.seconds),
      test("the round measurement sees a pinned seed") {
        // The control for the test above: with the seed held for the whole run,
        // which is the leak, every sampled round must still be reachable. This
        // is what shows a low count there is a real result and not a chaser
        // that has lost the chain.
        for {
          res <- roundsReachableDuringRun(1, pinSeed = true)
          (sampled, alive) = res
        } yield assertTrue(sampled == RoundCount / SampleEvery, alive == sampled)
      } @@ TestAspect.timeout(120.seconds),
      test("a callback hung inside a batched claim retains only its claim") {
        // See `Dispatcher.continueAfter`. `n = 512` never batches a round this
        // size, so it holds nothing either way and serves as the in-suite
        // comparison.
        for {
          par <- reachableWithHungCallback(f => source.mapZIOParUnordered(64)(f).runDrain)
          unbatched <- reachableWithHungCallback(f => source.runForeachPar(512, 16)(f))
          batched <- reachableWithHungCallback(f => source.runForeachPar(64, 16)(f))
          few <- reachableWithHungCallback(f => source.runForeachPar(8, 16)(f))
          _ <- ZIO.succeed(
            println(
              s"[retention] hung callback: mapZIOParUnordered=$par runForeachPar(512)=$unbatched runForeachPar(64)=$batched runForeachPar(8)=$few"
            )
          )
          // One chunk of slack for the round being dispatched when measured,
          // and one for the chunk the stream machinery holds.
        } yield assertTrue(batched <= par + 2 * chunkSz, few <= par + 2 * chunkSz, unbatched <= par + 2 * chunkSz)
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(120.seconds),
      test("an idle stream does not keep the drained round") {
        // See `Dispatcher.release`. The baseline is `runForeach`, which keeps
        // the stream machinery's own last chunk; that one is not ours to
        // release.
        for {
          seq <- reachableWhileIdle((s, f) => s.runForeach(f))
          four <- reachableWhileIdle((s, f) => s.runForeachPar(4, 16)(f))
          many <- reachableWhileIdle((s, f) => s.runForeachPar(64, 16)(f))
          _ <- ZIO.succeed(println(s"[retention] idle: runForeach=$seq runForeachPar(4)=$four runForeachPar(64)=$many"))
        } yield assertTrue(four <= seq + chunkSz / 2, many <= seq + chunkSz / 2)
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(120.seconds)
    )
}
