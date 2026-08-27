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

import java.lang.ref.WeakReference

/**
 * Rounds are linked forward through `Round#next`, and the seed round is captured
 * by the worker closures for the whole run. If a drained round keeps its chunk,
 * the seed transitively pins every chunk the run has ever pulled, so retention
 * grows with the length of the stream instead of being bounded by `bufferSize`.
 * That is invisible to correctness tests — every element is still visited
 * exactly once — and shows up only as an OOM on a long stream with large
 * elements.
 *
 * These tests measure reachability directly with weak references. The run is
 * held open mid-flight by blocking one callback on the last element, so the
 * worker pool is live and the round chain fully built at the moment of
 * measurement. `runForeach` and `mapZIOParUnordered` serve as controls: both
 * retain only the element they are blocked on.
 */
object RetentionSpec extends ZIOSpecDefault {

  private final class Payload(val id: Int) {
    // Large enough that retaining the whole stream is an OOM rather than a
    // curiosity, and that the GC has an incentive to actually collect.
    val filler = new Array[Byte](1024)
  }

  private val total    = 20000
  private val chunkSz  = 100
  private val sampleOf = 100
  // Blocking on the final element keeps the run in flight after every earlier
  // element has been processed.
  private val blockAt  = total - 1

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
      refs    <- Ref.make(List.empty[WeakReference[Payload]])
      blocked <- Promise.make[Nothing, Unit]
      release <- Promise.make[Nothing, Unit]
      f = (p: Payload) =>
            if (p.id == blockAt) blocked.succeed(()) *> release.await
            else if (p.id % sampleOf == 0) refs.update(new WeakReference(p) :: _)
            else ZIO.unit
      fiber <- consume(f).fork
      _     <- blocked.await
      // Let the remaining workers finish everything they can.
      _     <- ZIO.sleep(500.millis)
      _     <- ZIO.succeed { java.lang.System.gc(); Thread.sleep(300); java.lang.System.gc() }
      rs    <- refs.get
      alive  = rs.count(_.get() != null)
      _     <- release.succeed(()) *> fiber.interrupt
    } yield (rs.size, alive)

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
          one   <- reachableDuringRun(f => source.runForeachPar(1, 16)(f))
          many  <- reachableDuringRun(f => source.runForeachPar(512, 16)(f))
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
      } @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
    )
}
