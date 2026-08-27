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

package me.jeffshaw.zio.methods

import zio._
import zio.stream.Take
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicInteger

/**
 * Dispatches the elements of chunk-granular [[Take]]s to a pool of worker fibers
 * at element granularity, without a chunk boundary barrier.
 *
 * This is the transport/dispatch split that powers `runForeachPar`: chunks are
 * moved cheaply through the queue (one box per chunk, not per element), but every
 * worker claims individual elements out of the current chunk via a shared atomic
 * cursor, so any number of workers can be busy on the same chunk. In particular a
 * single chunk of `>= n` elements keeps all `n` workers busy — the failure mode
 * that a whole-chunk-per-worker design suffers from.
 *
 * ==Protocol==
 *
 * A [[ChunkCursorDistributor.Round]] holds the current chunk, an
 * [[AtomicInteger]] cursor, and a `Promise` for the next round. A worker reads
 * the current round and does `i = cursor.getAndIncrement()`:
 *
 *   - `i < chunk.length`: run `f(chunk(i))`, then loop on the same round.
 *   - `i == chunk.length`: this worker is the ''designated fetcher'' — exactly
 *     one worker observes the boundary value, by construction. It pulls the next
 *     [[Take]] from `fetch` and publishes the resulting round via `next`. For a
 *     data round it then loops on it; for a terminal round it returns instead
 *     (reporting the cause first, if the terminal is a failure), so it never
 *     re-observes the terminal it just published.
 *   - `i > chunk.length`: another worker is fetching; await `next`, then loop on
 *     the round it published.
 *
 * A terminal [[Take]] (end-of-stream or failure) yields a ''terminal round''.
 * Any worker reaching one stops, because `loop` checks `round.terminal` before
 * touching the cursor: the worker that published it loops onto it and returns,
 * and every worker awaiting the *previous* round's `next` receives it and hits
 * the same check. A terminal round's own `next` is therefore never awaited and
 * never completed; it exists only to fill the field. This makes every worker
 * converge to termination without any worker blocking on a promise that nobody
 * will complete.
 *
 * A terminal round carries no cause. The cause of a failing terminal is reported
 * to `onError` once, by the fetcher that pulled it, so a single upstream failure
 * produces a single cause however large `n` is — matching
 * [[zio.stream.ZChannel#mapOutZIOParUnordered]], where the lone pull loop plays
 * the same role.
 *
 * ==Memory visibility==
 *
 * A non-fetcher worker learns of a new round only by awaiting `next`, and the
 * fetcher completes `next` only after fully constructing the round. Promise
 * completion/await establishes a happens-before edge, so every field the fetcher
 * wrote (chunk contents, the fresh cursor, the fresh `next` promise) is visible
 * to awaiters. Within a round, `cursor` is an `AtomicInteger`, so element claims
 * are linearized and no index is handed out twice.
 */
private[methods] object ChunkCursorDistributor {

  /**
   * One round of dispatch over a single chunk. A ''terminal'' round is a pure
   * stop signal with an empty `chunk`: it carries no cause, because the cause of
   * a failing terminal is reported once by the fetcher that pulled it, never by
   * the workers that later observe the round.
   *
   * `chunk` is a `var` so a drained round can release it. Rounds are linked
   * forward through `next` — round k's promise resolves to round k+1 — so a
   * reference to any one round transitively reaches every later round. The seed
   * round is captured by the worker closures for the whole run, so without
   * releasing, every chunk the run has ever pulled stays reachable: retention
   * grows with the length of the stream rather than being bounded by
   * `bufferSize`. Clearing the field once the round can hand out no more
   * elements keeps the small round objects chained while letting the large
   * payload go.
   */
  private final class Round[E, A](
    @volatile var chunk: Chunk[A],
    val cursor: AtomicInteger,
    val next: Promise[Nothing, Round[E, A]],
    val terminal: Boolean
  )

  private object Round {
    def data[E, A](chunk: Chunk[A]): Round[E, A] =
      new Round(chunk, new AtomicInteger(0), makePromise[E, A], terminal = false)

    def terminal[E, A]: Round[E, A] =
      // A terminal round's `next` is never awaited: `loop` checks `terminal`
      // before touching the cursor, so a worker that loops onto a terminal round
      // stops immediately, and a worker awaiting the *previous* round's `next`
      // receives this round and then hits that same check. The promise is
      // therefore never completed and never read; it exists only to fill the
      // field.
      new Round[E, A](Chunk.empty, new AtomicInteger(0), makePromise[E, A], terminal = true)

    private def makePromise[E, A]: Promise[Nothing, Round[E, A]] =
      Promise.unsafe.make[Nothing, Round[E, A]](FiberId.None)(Unsafe)
  }

  /**
   * Runs the element-dispatch loop across `n` worker fibers.
   *
   *   - `fetch` pulls the next chunk-granular [[Take]] (typically a `Queue#take`
   *     or a channel pull). It is invoked by whichever worker becomes the
   *     designated fetcher, exactly once per chunk.
   *   - `f` is the per-element callback; each worker runs at most one `f` at a
   *     time, so global concurrency is bounded by `n`.
   *   - `onError` is invoked to record a cause; recording must be
   *     idempotent/accumulating. Each distinct failure is reported exactly once:
   *     a failure from `f` by the worker that ran it, and a failing terminal by
   *     the fetcher that pulled it. Workers that merely observe the resulting
   *     terminal round do not re-report it, so one upstream failure yields one
   *     cause regardless of `n`. Fail-fast interruption of in-flight `f`
   *     invocations is the caller's responsibility, via scope interruption, which
   *     matches the existing topology.
   *
   * The returned effect completes when every worker has observed a terminal
   * round.
   */
  def run[R, E <: E1, E1, A](
    n: Int,
    fetch: ZIO[R, Nothing, Take[E, A]],
    f: A => ZIO[R, E1, Any],
    onError: Cause[E1] => ZIO[R, Nothing, Unit]
  )(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed {
      // Seed round: an already-exhausted placeholder. Every worker's first
      // `getAndIncrement` yields i >= 0 == length; exactly the boundary observer
      // (i == 0) performs the initial fetch, the rest await.
      val seed = Round.data[E, A](Chunk.empty)

      // Publishes the round the fetcher just built to the workers awaiting it.
      // `Promise#done` is the public equivalent of the internal
      // `promise.unsafe.done`: it performs the identical `completeWith`, wrapped
      // in a single `ZIO.succeed`. That wrapper costs one effect node per
      // *chunk*, never per element, so it is off the hot path.
      def publish(round: Round[E, A], next: Round[E, A]): ZIO[Any, Nothing, Unit] =
        round.next.done(Exit.succeed(next)).unit

      // Releases a round's chunk once it can hand out no more elements. Only the
      // designated fetcher calls this, and only after it has observed the
      // boundary (`i == length`), so every element has already been claimed.
      // Workers still running `f` on a claimed element do not touch `chunk`
      // again: `loop` reads the element out of a local before invoking `f`. The
      // field is `@volatile`, so a worker that re-enters `loop` on this round
      // either sees the chunk (and its cursor is past the end, sending it to the
      // await branch) or sees null and is likewise past the end.
      def release(round: Round[E, A]): Unit =
        round.chunk = null.asInstanceOf[Chunk[A]]

      // A `ZIO.whileLoop` version of this loop was implemented and reverted: it
      // cut allocation by 23-38% while costing ~30% throughput, with
      // non-overlapping error bars. Throughput is the objective and allocation
      // only a diagnostic, so the recursive loop wins. The per-iteration graph
      // rebuilding that `whileLoop` avoids is evidently cheap enough for the
      // JIT to handle — consistent with hoisting the worker closures out of the
      // loop also measuring as a no-op. Don't retry either without a benchmark.
      def loop(round: Round[E, A]): ZIO[R, Nothing, Unit] =
        // A terminal round only signals "stop". The cause, if any, was already
        // reported once by the fetcher that pulled it, so workers arriving here
        // must not report it again.
        if (round.terminal) Exit.unit
        else {
          // Read the chunk once. A drained round's `chunk` is nulled by the
          // fetcher, and a worker can re-enter `loop` on such a round; reading
          // into a local keeps the length checks and the element read consistent
          // with each other regardless of when that happens.
          val chunk  = round.chunk
          val length = if (chunk eq null) 0 else chunk.length
          val i      = round.cursor.getAndIncrement()
          if (i < length)
            // The element is read out of the local before `f` runs, so `f` never
            // reaches back into the round.
            f(chunk(i)).foldCauseZIO(onError, _ => loop(round))
          else if (i == length && (chunk ne null))
            // Designated fetcher.
            fetch.flatMap { take =>
              take.exit.foldExit(
                cause =>
                  Cause.flipCauseOption(cause) match {
                    case None =>
                      publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round))
                    case Some(c) =>
                      // The fetcher is the sole reporter of a terminal cause:
                      // the round it publishes carries only the stop signal.
                      publish(round, Round.terminal[E, A]) *> ZIO.succeed(release(round)) *> onError(c)
                  },
                chunk => {
                  val nextRound = Round.data[E, A](chunk)
                  // Publish first, then drop this round's chunk: the successor
                  // is what keeps the run moving, and after it is published no
                  // worker can claim from this round again. This is what stops
                  // the seed round from transitively pinning the whole stream.
                  publish(round, nextRound) *> ZIO.succeed(release(round)) *> loop(nextRound)
                }
              )
            }
          else
            // Someone else is fetching; wait for the published round.
            round.next.await.flatMap(loop)
        }

      ZIO.foreachParDiscard(1 to n)(_ => loop(seed)).withParallelism(n)
    }
}
