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
 *
 * Fusing is the common case, not an edge case. It is tempting to read
 * "`takeBetween` suspends only when the queue is empty" as implying that
 * workers outrun an in-memory producer and each fetch therefore sees one chunk;
 * measurement says otherwise. Instrumenting the batch size a fetch observes,
 * with a no-op `f` over `ZStream.fromChunks` and the default `bufferSize` of
 * 16, 52-96% of fetches saw two or more chunks, averaging 3-14. The producer
 * refills the queue while the workers drain a multi-chunk round, so the steady
 * state is a populated queue even when `f` is free.
 *
 * [[fuse]] is therefore on the hot path, and its `flatMap` is not an accident.
 * `ChunkLike.flatMap` collects the source chunks, allocates one
 * `Array.ofDim(total)`, and fills it with one bulk `System.arraycopy` per
 * chunk, yielding the flat array that `Dispatcher.loop`'s per-element
 * `chunk(i)` wants. Two alternatives were implemented and measured against
 * `FetchPathBenchmark`, and both lost badly enough to revert:
 *
 *   - a hand-rolled `ChunkBuilder` sized in one pass: -58% at 512-element
 *     chunks, because it appends through the builder instead of bulk-copying;
 *   - `++`, which copies nothing and links the chunks into a `Chunk.Concat`
 *     tree: -47% at 512-element chunks, and still -8 to -13% at one element per
 *     chunk where there is nearly nothing to copy. That last point is the
 *     informative one: it isolates the loss to `Concat.apply`'s per-element
 *     tree descent replacing a flat array read, not to the copy.
 *
 * A third attempt went the other way: skip fusing when the head chunk is
 * already large enough to be a round on its own (`n * 8` elements), on the
 * theory that the copy then buys nothing. It lost by 26-61% wherever it
 * engaged, worst at `n = 64` with 512-element chunks. That is the informative
 * direction: a lone 512-element chunk gives 64 workers eight elements each, so
 * the round boundary (a promise completion and up to `n - 1` worker wakes)
 * arrives every eight elements per worker, and fusing sixteen chunks makes it
 * sixteen times rarer. The losses scale with `n`, which is the wake-herd
 * signature.
 *
 * So the copy is cheap and the round boundary is expensive, and all three
 * results agree on that ordering. Any future attempt here needs to keep the
 * fused round a flat chunk, and to fuse at least as eagerly as this does.
 */
private[stream] final class BatchingFetch[E, A] private (
  queue: Queue[Take[E, A]],
  batchMax: Int,
  fuseTarget: Int
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
      else
        queue.takeBetween(1, batchMax).flatMap { takes =>
          // `batchMax` bounds the batch in *chunks*, but what a round needs is
          // elements: at one element per chunk the default of 16 yields a
          // 16-element round, which is the regime measured ~20x slower than
          // 64-element chunks at equal element count.
          //
          // So when the batch is element-poor, keep draining. `takeAll` never
          // blocks (it returns empty if the queue is dry), so this cannot add
          // latency or stall a slow producer, and it is skipped entirely once
          // the first take already clears the target, which is the common case
          // for chunks of any real size.
          //
          // Measured on `FetchPathBenchmark` at one element per chunk, the
          // regime this targets, re-run at `-f 5 -wi 10 -i 10`:
          //
          //   n = 4:  4.211 +/- 0.069 to 4.382 +/- 0.057 ops/s, +4.1%
          //   n = 64: 2.789 +/- 0.044 to 3.052 +/- 0.068 ops/s, +9.4%
          //
          // Both separate, with fork spreads under 10%. The gain is larger at
          // `n = 64` because the target scales with `n`: a 16-chunk batch holds
          // 16 elements against a target of 512 there, versus 32 at `n = 4`, so
          // the drain has more to add. At 64- and 512-element chunks the first
          // take already meets the target and every point was flat, which is
          // the control this needed to pass.
          if (elementsAtLeast(takes, fuseTarget)) Exit.succeed(split(takes))
          else queue.takeAll.map(more => split(if (more.isEmpty) takes else takes ++ more))
        }
    }

  /**
   * Whether `takes` carries at least `target` elements, stopping as soon as it
   * does.
   *
   * Short-circuiting matters: this runs per round, and for chunks of any real
   * size the first take settles it.
   */
  private def elementsAtLeast(takes: Chunk[Take[E, A]], target: Int): Boolean = {
    var total = 0
    var i = 0
    while (i < takes.length && total < target) {
      total += (takes(i).exit match {
        case Exit.Success(chunk) => chunk.length
        // A terminal contributes nothing, and stops the scan: there is no point
        // draining further for elements that cannot be dispatched before it.
        case _ => return total >= target
      })
      i += 1
    }
    total >= target
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
        case _ => Chunk.empty // unreachable: terminals are split off by `split`
      }))

  /**
   * How many elements a fused round should hold before the fetcher stops
   * draining the queue for more.
   *
   * Matched to `Round.ClaimsPerWorker`: below `n * 8` elements `Round.strideFor`
   * pins the stride to 1, so a round under this target gets no claim batching at
   * all. Draining up to it is what lets the stride engage; past it there is
   * nothing further to win, and `batchMax` still bounds the batch in chunks.
   */
  private final val FuseTargetClaimsPerWorker = 8

  /**
   * Builds the per-run fetcher over `queue`, batching up to `bufferSize` chunks
   * and draining toward a round of `n * 8` elements.
   */
  def apply[E, A](queue: Queue[Take[E, A]], bufferSize: Int, n: Int): BatchingFetch[E, A] =
    new BatchingFetch[E, A](
      queue,
      bufferSize max 1,
      // In `Long` then clamped: `n` is caller-supplied and routinely in the
      // thousands, where `n * 8` in `Int` would overflow to a negative target
      // and make every batch look like it had already met it.
      ((n.toLong max 1L) * FuseTargetClaimsPerWorker min Int.MaxValue.toLong).toInt
    )

  /**
   * The per-run fetch effect over `queue`. Built once here, so a round pays only
   * the suspension and the pull.
   */
  def effect[E, A](queue: Queue[Take[E, A]], bufferSize: Int, n: Int)(implicit
    trace: Trace
  ): ZIO[Any, Nothing, Take[E, A]] =
    apply[E, A](queue, bufferSize, n).effect
}
