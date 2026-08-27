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
import zio.concurrent.CountdownLatch
import zio.stream.ZStream
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky

object RunForeachParSpec extends ZIOSpecDefault {

  def spec =
    suite("runForeachPar")(
      test("visits every element") {
        checkN(10)(Gen.small(Gen.listOfN(_)(Gen.byte))) { data =>
          for {
            ref <- Ref.make(Set.empty[Byte])
            _   <- ZStream.fromIterable(data).runForeachPar(8)(a => ref.update(_ + a))
            res <- ref.get
          } yield assert(res)(equalTo(data.toSet))
        }
      },
      test("failure of the callback is failure") {
        val effect = ZStream.fromIterable(0 to 3).runForeachPar(10)(_ => ZIO.fail("fail"))
        assertZIO(effect.exit)(fails(equalTo("fail")))
      } @@ nonFlaky @@ TestAspect.diagnose(10.seconds),
      test("propagates error of original stream") {
        for {
          fiber <- (ZStream(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ++ ZStream.fail(new Throwable("Boom")))
                     .runForeachPar(2)(_ => ZIO.sleep(1.second))
                     .fork
          _    <- TestClock.adjust(5.seconds)
          exit <- fiber.await
        } yield assert(exit)(fails(hasMessage(equalTo("Boom"))))
      },
      test("interruption propagation") {
        for {
          interrupted <- Ref.make(false)
          latch       <- Promise.make[Nothing, Unit]
          fib <- ZStream(())
                   .runForeachPar(1)(_ => (latch.succeed(()) *> ZIO.never).onInterrupt(interrupted.set(true)))
                   .fork
          _      <- latch.await
          _      <- fib.interrupt
          result <- interrupted.get
        } yield assert(result)(isTrue)
      },
      test("interrupts pending tasks when one of the tasks fails") {
        for {
          interrupted <- Ref.make(0)
          latch1      <- Promise.make[Nothing, Unit]
          latch2      <- Promise.make[Nothing, Unit]
          result <- ZStream(1, 2, 3)
                      .runForeachPar(3) {
                        case 1 => (latch1.succeed(()) *> ZIO.never).onInterrupt(interrupted.update(_ + 1))
                        case 2 => (latch2.succeed(()) *> ZIO.never).onInterrupt(interrupted.update(_ + 1))
                        case _ => latch1.await *> latch2.await *> ZIO.fail("Boom")
                      }
                      .exit
          count <- interrupted.get
        } yield assert(count)(equalTo(2)) && assert(result)(fails(equalTo("Boom")))
      } @@ nonFlaky(500),
      test("parallelism is not exceeded") {
        val iterations = 1000
        checkAll(Gen.fromIterable(Chunk(4, 16, 32, 64))) { parallelism =>
          for {
            latch <- CountdownLatch.make(parallelism + 1)
            f <- ZStream
                   .range(0, iterations)
                   .runForeachPar(parallelism)(_ => latch.countDown *> latch.await)
                   .fork
            _     <- Live.live(latch.count.delay(100.micros)).repeatUntil(_ == 1)
            _     <- latch.countDown
            count <- latch.count
            _     <- f.join
          } yield assertTrue(count == 0)
        }
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("single chunk saturates all workers (chunk transport, element dispatch)") {
        // A stream of a *single* chunk with >= n elements must keep all n
        // workers busy simultaneously: this proves chunk-granular transport
        // with element-granular dispatch honors the concurrency contract,
        // the failure mode that a whole-chunk-per-worker design suffers.
        val parallelism = 16
        for {
          latch <- CountdownLatch.make(parallelism)
          _ <- ZStream
                 .fromChunk(Chunk.fromIterable(1 to parallelism))
                 .runForeachPar(parallelism)(_ => latch.countDown *> latch.await)
        } yield assertCompletes
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("fetcher election: every element visited exactly once under many small chunks") {
        // Many small chunks (sizes 1 and 2) under high n stress the
        // `i == length` fetcher-election boundary. Assert every element is
        // visited exactly once.
        val chunks = Chunk.fromIterable((0 until 500).map { i =>
          if (i % 2 == 0) Chunk.single(i) else Chunk(i, i + 1000)
        })
        val expected = chunks.flatten.toSet
        for {
          ref <- Ref.make(Set.empty[Int])
          _   <- ZStream.fromChunks(chunks: _*).runForeachPar(64)(a => ref.update(_ + a))
          res <- ref.get
        } yield assert(res)(equalTo(expected))
      } @@ nonFlaky(50),
      test("terminal end mid-flight: no element dropped, all workers finish") {
        // End-of-stream arrives while workers are mid-element. No preceding
        // element may be dropped, and the run must terminate.
        val n     = 32
        val total = 2000
        for {
          count <- Ref.make(0)
          _ <- ZStream
                 .range(0, total)
                 .runForeachPar(n)(_ => count.update(_ + 1))
          res <- count.get
        } yield assertTrue(res == total)
      } @@ nonFlaky(50),
      test("visits every element exactly once") {
        // A `Set` cannot distinguish "visited" from "visited twice", which is
        // exactly what the shared atomic cursor exists to prevent. Count each
        // element's visits so a duplicate dispatch is a failure.
        val total = 5000
        for {
          counts <- Ref.make(Map.empty[Int, Int])
          _ <- ZStream
                 .range(0, total)
                 .runForeachPar(32)(a => counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1)))
          res <- counts.get
        } yield assertTrue(res.size == total) && assertTrue(res.values.forall(_ == 1))
      } @@ nonFlaky(20),
      test("visits every element exactly once with many small chunks") {
        // Small chunks under high `n` maximize how often the `i == length`
        // fetcher-election boundary is crossed. A duplicate or dropped element
        // at a round handoff shows up as a count != 1.
        val chunks = Chunk.fromIterable((0 until 500).map { i =>
          if (i % 2 == 0) Chunk.single(i) else Chunk(i, i + 1000)
        })
        val expected = chunks.flatten
        for {
          counts <- Ref.make(Map.empty[Int, Int])
          _ <- ZStream
                 .fromChunks(chunks.toSeq: _*)
                 .runForeachPar(64)(a => counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1)))
          res <- counts.get
        } yield assertTrue(res.size == expected.length) && assertTrue(res.values.forall(_ == 1))
      } @@ nonFlaky(50),
      test("visits every element exactly once for each bufferSize") {
        // Exercises the `bufferSize` overload, including `bufferSize == 1`,
        // where `batchMax == 1` disables batch fusion entirely, and larger
        // sizes, where several chunks are fused into one round.
        val total = 2000
        checkAll(Gen.fromIterable(Chunk(1, 2, 16, 128))) { bufferSize =>
          for {
            counts <- Ref.make(Map.empty[Int, Int])
            _ <- ZStream
                   .range(0, total, chunkSize = 8)
                   .runForeachPar(16, bufferSize)(a => counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1)))
            res <- counts.get
          } yield assertTrue(res.size == total) && assertTrue(res.values.forall(_ == 1))
        }
      } @@ nonFlaky(20),
      test("terminal fused mid-batch is not lost (pendingTerminal)") {
        // Fill the queue with several chunks *and* the end-of-stream `Take`
        // before any worker fetches, so a single `takeBetween` batch contains
        // both data and the terminal. The terminal must be parked in
        // `pendingTerminal` and delivered after the fused data round drains:
        // no element may be dropped, and the run must still terminate.
        val total = 64
        for {
          gate     <- Promise.make[Nothing, Unit]
          produced <- Promise.make[Nothing, Unit]
          counts   <- Ref.make(Map.empty[Int, Int])
          fiber <- ZStream
                     .range(0, total, chunkSize = 4)
                     // Fires as the producer emits the final element, so the
                     // wait below is deterministic rather than a timing guess.
                     // `ensuring` would deadlock here: it runs at scope close,
                     // which cannot happen until the gated workers finish.
                     .tap(a => produced.succeed(()).when(a == total - 1))
                     .runForeachPar(4, 1024) { a =>
                       // Hold every worker until the producer has pushed the
                       // whole stream, terminal included, into the queue.
                       gate.await *> counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1))
                     }
                     .fork
          // The producer is unblocked (bufferSize far exceeds the chunk count),
          // so it runs to completion before the workers are released.
          _   <- produced.await
          _   <- gate.succeed(())
          _   <- fiber.join
          res <- counts.get
        } yield assertTrue(res.size == total) && assertTrue(res.values.forall(_ == 1))
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("terminal failure fused mid-batch is not lost") {
        // Same as above, but the terminal is a failure rather than
        // end-of-stream: it must survive being parked and still fail the run.
        for {
          gate     <- Promise.make[Nothing, Unit]
          produced <- Promise.make[Nothing, Unit]
          fiber <- (ZStream.range(0, 64, chunkSize = 4).tap(a => produced.succeed(()).when(a == 63)) ++
                     ZStream.fail("boom"))
                     .runForeachPar(4, 1024)(_ => gate.await)
                     .exit
                     .fork
          _    <- produced.await
          _    <- gate.succeed(())
          exit <- fiber.join
        } yield assert(exit)(fails(equalTo("boom")))
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("a concurrent failure is always recorded") {
        // How *many* concurrent failures get recorded is a race: the first
        // failure fires `errorSignal`, and the interruption that follows can
        // beat the second worker's `onError`. The base combinator races the
        // same way, so asserting on the count would pin a scheduling outcome.
        //
        // What is guaranteed: recording is sequenced before `errorSignal.done`
        // inside `fail`, and interruption only ever follows that signal, so
        // whichever failure fired the signal has already committed. Hence at
        // least one failure, and nothing but the expected failures.
        for {
          latch <- CountdownLatch.make(2)
          exit <- ZStream(1, 2)
                    .runForeachPar(2)(a => latch.countDown *> latch.await *> ZIO.fail(s"boom-$a"))
                    .exit
          failures = exit.causeOption.toList.flatMap(_.failures)
        } yield assertTrue(failures.nonEmpty) &&
          assertTrue(failures.toSet.subsetOf(Set("boom-1", "boom-2")))
      } @@ TestAspect.jvmOnly @@ nonFlaky(50),
      test("both concurrent failures are reachable in one exit") {
        // The relaxed assertion above cannot distinguish accumulation
        // (`failure.unsafe.update(_ && cause)`) from replacement: keeping only
        // the first failure also yields a one-element subset. Accumulation is
        // instead pinned as a *reachability* property — recording both is a
        // possible outcome, which a keep-first implementation could never
        // produce, so the retry would exhaust its bound and fail.
        val attempt =
          for {
            latch <- CountdownLatch.make(2)
            exit <- ZStream(1, 2)
                      .runForeachPar(2)(a => latch.countDown *> latch.await *> ZIO.fail(s"boom-$a"))
                      .exit
          } yield exit.causeOption.toList.flatMap(_.failures).toSet
        for {
          seen <- ZIO.iterate((Set.empty[String], 0))(s => s._1 != Set("boom-1", "boom-2") && s._2 < 200) {
                    case (_, attempts) => attempt.map(fs => (fs, attempts + 1))
                  }
        } yield assertTrue(seen._1 == Set("boom-1", "boom-2"))
      } @@ TestAspect.jvmOnly,
      test("empty stream completes") {
        // The seed round's designated fetcher immediately receives a terminal
        // `Take`; every worker must converge to termination with no element.
        for {
          visited <- Ref.make(0)
          _       <- ZStream.empty.runForeachPar(8)(_ => visited.update(_ + 1))
          res     <- visited.get
        } yield assertTrue(res == 0)
      } @@ nonFlaky(20),
      test("non-positive n consumes the stream sequentially, in order") {
        // Only `n <= 0` short-circuits to `ZStream#runForeach`. Such an `n` is
        // treated as sequential rather than rejected; pin that behavior.
        checkAll(Gen.fromIterable(Chunk(-1, 0))) { n =>
          for {
            visited <- Ref.make(Chunk.empty[Int])
            _       <- ZStream.range(0, 100).runForeachPar(n)(a => visited.update(_ :+ a))
            res     <- visited.get
          } yield assertTrue(res == Chunk.fromIterable(0 until 100))
        }
      },
      test("n == 1 visits every element exactly once") {
        // `n == 1` takes the forked path, so element order is not guaranteed —
        // only that every element is visited exactly once, under both overloads.
        val total = 500
        checkAll(Gen.fromIterable(Chunk(1, 16))) { bufferSize =>
          for {
            counts <- Ref.make(Map.empty[Int, Int])
            _ <- ZStream
                   .range(0, total, chunkSize = 8)
                   .runForeachPar(1, bufferSize)(a => counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1)))
            res <- counts.get
          } yield assertTrue(res.size == total) && assertTrue(res.values.forall(_ == 1))
        }
      } @@ nonFlaky(20),
      test("n == 1 runs one invocation of f at a time") {
        // The concurrency bound must still hold on the forked path: a second
        // invocation entering while the first is in flight is a contract
        // violation, so track the observed maximum overlap.
        for {
          inFlight <- Ref.make(0)
          maxSeen  <- Ref.make(0)
          _ <- ZStream
                 .range(0, 200, chunkSize = 8)
                 .runForeachPar(1) { _ =>
                   ZIO.acquireReleaseWith(
                     inFlight.updateAndGet(_ + 1).flatMap(c => maxSeen.update(_ max c))
                   )(_ => inFlight.update(_ - 1))(_ => ZIO.yieldNow)
                 }
          res <- maxSeen.get
        } yield assertTrue(res == 1)
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("n == 1 overlaps stream consumption with f") {
        // The reason `n == 1` no longer degrades to `runForeach`: the producer
        // fills the buffer while `f` runs. With a buffer of 8 and a gated `f`,
        // the stream must be pulled well past the first element before the
        // first invocation of `f` is allowed to complete — which cannot happen
        // if pulling and `f` share one fiber.
        for {
          pulled <- Ref.make(0)
          gate   <- Promise.make[Nothing, Unit]
          fiber <- ZStream
                     .range(0, 100, chunkSize = 1)
                     .tap(_ => pulled.update(_ + 1))
                     .runForeachPar(1, 8)(_ => gate.await)
                     .fork
          // Wait until the producer has run ahead of the blocked `f`.
          _   <- pulled.get.repeatUntil(_ > 1)
          res <- pulled.get
          _   <- gate.succeed(())
          _   <- fiber.join
        } yield assertTrue(res > 1)
      } @@ TestAspect.jvmOnly @@ nonFlaky(20),
      test("a defect in the callback is not swallowed") {
        // `Cause.empty` is the sentinel for a clean end-of-stream, and defects
        // travel the same `foldCauseZIO` path as typed failures.
        val boom   = new RuntimeException("die")
        val effect = ZStream.range(0, 100).runForeachPar(8)(_ => ZIO.die(boom))
        assertZIO(effect.exit)(dies(equalTo(boom)))
      } @@ nonFlaky(20),
      test("a defect in the stream is not swallowed") {
        val boom   = new RuntimeException("die")
        val effect = (ZStream.range(0, 100) ++ ZStream.die(boom)).runForeachPar(8)(_ => ZIO.unit)
        assertZIO(effect.exit)(dies(equalTo(boom)))
      } @@ nonFlaky(20),
      test("the environment is available to the callback") {
        // `provideSomeEnvironment[R1](_.add[Scope](childScope))` on the
        // producer must not strip `R1` from the workers' environment.
        val total = 100
        val effect =
          for {
            _   <- ZStream.range(0, total).runForeachPar(8)(_ => ZIO.serviceWithZIO[Counter](_.increment))
            res <- ZIO.serviceWithZIO[Counter](_.get)
          } yield assertTrue(res == total)
        effect.provide(Counter.layer)
      } @@ nonFlaky(20),
      test("fail-fast after terminal failure round") {
        // A failure must promptly short-circuit the run even when it arrives
        // as a terminal round after successful elements.
        val effect =
          (ZStream.range(0, 100) ++ ZStream.fail("boom"))
            .runForeachPar(8)(_ => ZIO.unit)
        assertZIO(effect.exit)(fails(equalTo("boom")))
      } @@ nonFlaky(50)
      // A bug in the round handoff (a terminal that is never delivered, a
      // promise left uncompleted) shows up as workers that never terminate.
      // Without a cap that hangs the run instead of reporting a failure. This
      // aspect applies per test, not to the suite as a whole, so keep it tight:
      // the slowest test here takes well under a second.
    ) @@ TestAspect.timeout(5.seconds)

  private trait Counter {
    def increment: UIO[Unit]
    def get: UIO[Int]
  }

  private object Counter {
    val layer: ULayer[Counter] =
      ZLayer(Ref.make(0).map { ref =>
        new Counter {
          override def increment: UIO[Unit] = ref.update(_ + 1)
          override def get: UIO[Int]        = ref.get
        }
      })
  }
}
