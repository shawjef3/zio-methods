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

package me.jeffshaw.zio.stream

import zio._
import zio.stream.Take
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicReference

/**
 * The fetcher's half of the `runForeachPar` protocol: pulls chunk-granular
 * [[Take]]s out of a queue in batches and fuses them into a single [[Take]], so
 * one dispatch round spans every chunk that was already buffered rather than
 * exactly one.
 *
 * The designated fetcher drains every chunk already buffered (at least one;
 * `takeBetween(1, max)` suspends only when the queue is empty) and fuses them
 * into a single `Take`, so one round spans up to `bufferSize` chunks instead of
 * one.
 *
 * Why: when `n` is much larger than the chunk size, a round has fewer elements
 * than workers, and every round publish wakes all overflow workers at once to
 * race for the next chunk — measured at ~28% of throughput at n = 16k-40k with
 * 2000-element chunks (5ms IO-like `f`). Fusing multiplies elements per round by
 * the number of buffered chunks, making the wake-herd boundary proportionally
 * rarer, while dispatch stays element-granular so load balance and the
 * concurrency contract are unchanged. When the queue holds a single chunk (the
 * n <= chunkSize regime), [[BatchingFetch.fuse]] returns it as-is and no copy is
 * made.
 *
 * A terminal `Take` inside the batch is split off and parked, to be delivered by
 * the *next* fetch after the fused data round drains. Visibility: only the
 * designated fetcher (unique per round, by cursor construction) touches it, and
 * successive fetchers are ordered by the round handoff; the [[AtomicReference]]
 * makes that independent of those details.
 *
 * One instance is built per run, and [[effect]] is likewise built once: a fetch
 * costs the `suspendSucceed` node, the parked-terminal read, the `takeBetween`,
 * and the fusing, and allocates nothing else per round.
 */
private[stream] final class BatchingFetch[E, A] private (
  queue: Queue[Take[E, A]],
  batchMax: Int
) {

  /**
   * Holds the terminal `Take`'s underlying `Exit` (`Take` is an `AnyVal`, so the
   * reference stores the boxed exit instead). `null` when no terminal is parked.
   */
  private[this] val pendingTerminal = new AtomicReference[Exit[Option[E], Chunk[A]]](null)

  /**
   * Splits a batch into the [[Take]] to hand out now and the terminal to park.
   *
   * Parks as a side effect rather than returning a pair, because a pair would
   * allocate once per round on the fetch path. The park is a field write on the
   * instance that is about to be read back by the next fetch, which is the same
   * lifetime the pair would have had.
   */
  private[stream] def split(takes: Chunk[Take[E, A]]): Take[E, A] = {
    val terminalIndex = takes.indexWhere(!_.exit.isSuccess)
    if (terminalIndex < 0) BatchingFetch.fuse(takes)
    else if (terminalIndex == 0) takes.head
    else {
      pendingTerminal.set(takes(terminalIndex).exit)
      BatchingFetch.fuse(takes.take(terminalIndex))
    }
  }

  /** The parked terminal, or `null` if none. Exposed for testing the park. */
  private[stream] def parked: Exit[Option[E], Chunk[A]] = pendingTerminal.get

  /**
   * The fetch effect handed to [[ChunkCursorDistributor]]. Call it once per run
   * and reuse the result, as [[BatchingFetch.apply]] does: the per-round cost is
   * then the suspension and the pull, not rebuilding the effect.
   */
  private[stream] def effect(implicit trace: Trace): ZIO[Any, Nothing, Take[E, A]] =
    ZIO.suspendSucceed {
      val parked = pendingTerminal.get
      if (parked ne null) Exit.succeed(Take(parked))
      else queue.takeBetween(1, batchMax).map(takes => split(takes))
    }
}

private[stream] object BatchingFetch {

  /**
   * Fuses a batch of data [[Take]]s into one. A single-element batch is returned
   * as-is, so the `n <= chunkSize` regime makes no copy at all.
   *
   * Terminals are split off by [[BatchingFetch.split]] before this is called, so
   * the non-success case is unreachable.
   */
  private[stream] def fuse[E, A](data: Chunk[Take[E, A]]): Take[E, A] =
    if (data.length == 1) data.head
    else
      Take.chunk(data.flatMap(_.exit match {
        case Exit.Success(chunk) => chunk
        case _                   => Chunk.empty // unreachable: terminals are split off by `split`
      }))

  /** Builds the per-run fetcher over `queue`, batching up to `bufferSize` chunks. */
  def apply[E, A](queue: Queue[Take[E, A]], bufferSize: Int): BatchingFetch[E, A] =
    new BatchingFetch[E, A](queue, bufferSize max 1)

  /**
   * The per-run fetch effect over `queue`. Built once here, so a round pays only
   * the suspension and the pull.
   */
  def effect[E, A](queue: Queue[Take[E, A]], bufferSize: Int)(implicit
    trace: Trace
  ): ZIO[Any, Nothing, Take[E, A]] =
    apply[E, A](queue, bufferSize).effect
}
