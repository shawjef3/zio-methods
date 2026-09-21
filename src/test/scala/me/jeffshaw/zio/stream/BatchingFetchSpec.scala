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
      case _                   => None
    }

  private def fetcher(takes: Take[String, Int]*): UIO[BatchingFetch[String, Int]] =
    Queue
      .bounded[Take[String, Int]](takes.length max 1)
      .tap(q => ZIO.foreachDiscard(takes)(q.offer))
      .map(BatchingFetch[String, Int](_, 1024))

  def spec =
    suite("BatchingFetch")(
      suite("fuse")(
        test("a single take is returned as-is, without copying") {
          val only  = data(1, 2, 3)
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
      suite("effect")(
        test("delivers the fused data round, then the parked terminal") {
          for {
            b      <- fetcher(data(1, 2), data(3), Take.end)
            first  <- b.effect
            second <- b.effect
          } yield assertTrue(elements(first).contains(Chunk(1, 2, 3))) &&
            assertTrue(elements(second).isEmpty)
        },
        test("a parked failing terminal survives to the next fetch") {
          for {
            b     <- fetcher(data(1), Take.fail("boom"))
            first <- b.effect
            // The terminal was parked mid-batch; the failure must still arrive.
            second <- b.effect
            cause = second.exit match {
                      case Exit.Failure(c) => Cause.flipCauseOption(c)
                      case _               => None
                    }
          } yield assertTrue(elements(first).contains(Chunk(1))) &&
            assertTrue(cause.exists(_.failures == List("boom")))
        },
        test("the parked terminal is delivered repeatedly, never consumed once") {
          // A worker that re-fetches after the terminal must keep seeing it,
          // rather than falling through to a queue pull that would block.
          for {
            b      <- fetcher(data(1), Take.end)
            _      <- b.effect
            second <- b.effect
            third  <- b.effect
          } yield assertTrue(elements(second).isEmpty) && assertTrue(elements(third).isEmpty)
        }
      )
    )
}
