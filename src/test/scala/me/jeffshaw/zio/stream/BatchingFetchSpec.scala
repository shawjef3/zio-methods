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

/**
 * Covers the batching fetcher's fusing and terminal splitting directly, as
 * ordinary calls over `Chunk[Take[E, A]]`.
 *
 * `RunForeachParSpec` reaches the same logic end to end, but only through a
 * two-promise gate and a `bufferSize` large enough to force a multi-chunk batch.
 * Those tests remain as integration backstops; these are the ones that state
 * what the split is supposed to do.
 */
object BatchingFetchSpec extends ZIOSpecDefault {

  private def data(values: Int*): Take[String, Int] =
    Take.chunk(Chunk.fromIterable(values))

  /** The elements a data `Take` carries, or `None` if it is terminal. */
  private def elements(take: Take[String, Int]): Option[Chunk[Int]] =
    take.exit match {
      case Exit.Success(chunk) => Some(chunk)
      case _ => None
    }

  /**
   * `n = 1`, so the fetcher's element target is 8. These tests use small takes
   * and assert on what a single fetch returns, so a low target keeps the
   * element-poor drain out of the way of what they are checking; the drain has
   * its own tests below.
   */
  private def fetcher(takes: Take[String, Int]*): UIO[BatchingFetch[String, Int]] =
    fetcherWithN(1, takes: _*)

  private def fetcherWithN(n: Int, takes: Take[String, Int]*): UIO[BatchingFetch[String, Int]] =
    Queue
      .bounded[Take[String, Int]](takes.length max 1)
      .tap(q => ZIO.foreachDiscard(takes)(q.offer))
      .map(BatchingFetch[String, Int](_, 1024, n))

  def spec =
    suite("BatchingFetch")(
      suite("fuse")(
        test("a single take is returned as-is, without copying") {
          val only = data(1, 2, 3)
          val fused = BatchingFetch.fuse(Chunk(only))
          // Identity, not just equality: the n <= chunkSize regime must make no
          // copy at all.
          assertTrue(elements(fused).get eq elements(only).get)
        },
        test("several takes fuse into one, in order") {
          val fused = BatchingFetch.fuse(Chunk(data(1, 2), data(3), data(4, 5)))
          assertTrue(elements(fused).contains(Chunk(1, 2, 3, 4, 5)))
        },
        test("empty data takes contribute nothing") {
          val fused = BatchingFetch.fuse(Chunk(data(), data(1, 2), data()))
          assertTrue(elements(fused).contains(Chunk(1, 2)))
        }
      ),
      suite("split")(
        test("a batch of only data parks nothing") {
          for {
            b <- fetcher()
            out = b.split(Chunk(data(1, 2), data(3)))
          } yield assertTrue(elements(out).contains(Chunk(1, 2, 3))) &&
            assertTrue(b.parked eq null)
        },
        test("a terminal at index 0 is returned alone and parks nothing") {
          for {
            b <- fetcher()
            out = b.split(Chunk(Take.end, data(1)))
          } yield assertTrue(elements(out).isEmpty) && assertTrue(b.parked eq null)
        },
        test("a terminal mid-batch is parked and the data ahead of it is fused") {
          for {
            b <- fetcher()
            out = b.split(Chunk(data(1, 2), data(3), Take.end))
          } yield assertTrue(elements(out).contains(Chunk(1, 2, 3))) &&
            assertTrue(b.parked ne null)
        },
        test("data after a mid-batch terminal is dropped, as the stream ended there") {
          for {
            b <- fetcher()
            out = b.split(Chunk(data(1), Take.end, data(99)))
          } yield assertTrue(elements(out).contains(Chunk(1)))
        }
      ),
      suite("element-poor drain")(
        test("an element-poor batch is topped up past bufferSize's chunk bound") {
          // Eight single-element chunks with batchMax = 2: `takeBetween` can
          // return at most 2, which is element-poor for n = 1 (target 8), so the
          // fetch drains the rest non-blockingly and one round carries all 8.
          for {
            q <- Queue.bounded[Take[String, Int]](16)
            _ <- ZIO.foreachDiscard(1 to 8)(i => q.offer(data(i)))
            b = BatchingFetch[String, Int](q, 2, 1)
            out <- b.effect
          } yield assertTrue(elements(out).contains(Chunk(1, 2, 3, 4, 5, 6, 7, 8)))
        },
        test("a batch already at target is not drained further") {
          // The first chunk alone meets the target, so the queue must be left
          // alone: the second chunk stays for the next fetch.
          for {
            q <- Queue.bounded[Take[String, Int]](16)
            _ <- q.offer(data(1, 2, 3, 4, 5, 6, 7, 8))
            _ <- q.offer(data(9))
            b = BatchingFetch[String, Int](q, 1, 1)
            first <- b.effect
            second <- b.effect
          } yield assertTrue(elements(first).contains(Chunk(1, 2, 3, 4, 5, 6, 7, 8))) &&
            assertTrue(elements(second).contains(Chunk(9)))
        },
        test("draining an empty queue adds nothing and does not block") {
          // One element-poor chunk and nothing behind it. `takeAll` returns
          // empty, so the round is what the first take held.
          for {
            q <- Queue.bounded[Take[String, Int]](16)
            _ <- q.offer(data(1))
            b = BatchingFetch[String, Int](q, 4, 1)
            out <- b.effect.timeoutFail("blocked")(5.seconds).either
          } yield assertTrue(out.isRight) &&
            assertTrue(out.toOption.flatMap(elements).contains(Chunk(1)))
        },
        test("a terminal picked up by the drain is still parked, not lost") {
          // The drain pulls the terminal in alongside data. It must be split off
          // and delivered after the data round, exactly as when `takeBetween`
          // returns it directly.
          for {
            q <- Queue.bounded[Take[String, Int]](16)
            _ <- q.offer(data(1))
            _ <- q.offer(data(2))
            _ <- q.offer(Take.end)
            b = BatchingFetch[String, Int](q, 1, 1)
            first <- b.effect
            second <- b.effect
          } yield assertTrue(elements(first).contains(Chunk(1, 2))) &&
            assertTrue(elements(second).isEmpty)
        }
      ),
      suite("effect")(
        test("delivers the fused data round, then the parked terminal") {
          for {
            b <- fetcher(data(1, 2), data(3), Take.end)
            first <- b.effect
            second <- b.effect
          } yield assertTrue(elements(first).contains(Chunk(1, 2, 3))) &&
            assertTrue(elements(second).isEmpty)
        },
        test("a parked failing terminal survives to the next fetch") {
          for {
            b <- fetcher(data(1), Take.fail("boom"))
            first <- b.effect
            // The terminal was parked mid-batch; the failure must still arrive.
            second <- b.effect
            cause = second.exit match {
              case Exit.Failure(c) => Cause.flipCauseOption(c)
              case _ => None
            }
          } yield assertTrue(elements(first).contains(Chunk(1))) &&
            assertTrue(cause.exists(_.failures == List("boom")))
        },
        test("the parked terminal is delivered repeatedly, never consumed once") {
          // A worker that re-fetches after the terminal must keep seeing it,
          // rather than falling through to a queue pull that would block.
          for {
            b <- fetcher(data(1), Take.end)
            _ <- b.effect
            second <- b.effect
            third <- b.effect
          } yield assertTrue(elements(second).isEmpty) && assertTrue(elements(third).isEmpty)
        }
      )
    )
}
