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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * One round of dispatch over a single chunk. A ''terminal'' round is a pure
 * stop signal with an empty `chunk`: it carries no cause, because the cause of
 * a failing terminal is reported once by the fetcher that pulled it, never by
 * the workers that later observe the round.
 *
 * `chunk` is a `var` so a drained round can release it. Rounds are linked
 * forward through `next` — round k's promise resolves to round k+1 — so a
 * reference to any one round transitively reaches every later round. The seed
 * round is held by the [[Dispatcher]] for the whole run, so without
 * releasing, every chunk the run has ever pulled stays reachable: retention
 * grows with the length of the stream rather than being bounded by
 * `bufferSize`. Clearing the field once the round can hand out no more
 * elements keeps the small round objects chained while letting the large
 * payload go.
 */
private[stream] final class Round[E, A](
  @volatile var chunk: Chunk[A],
  val cursor: AtomicInteger,
  val next: Promise[Nothing, Round[E, A]],
  val terminal: Boolean,
  /**
   * How many contiguous elements one claim reserves. See [[Round.strideFor]];
   * a stride of `1` reproduces the one-element-per-atomic behavior exactly.
   */
  val stride: Int,
  /**
   * Elects the round's single designated fetcher when `stride > 1`: the first
   * worker to find the cursor at or past the end wins it by CAS.
   *
   * `null` for a stride-1 round, where it is neither allocated nor read. There
   * the bases are consecutive, so exactly one worker lands on `i == length` and
   * that implicit test elects it for free. Batched claims skip bases — a
   * stride-8 claim on a 10-element round leaves the cursor at 16 — so no base
   * need ever equal `length`, and the implicit test would elect nobody and
   * deadlock the run; only there is the extra atomic worth paying, and only
   * there is the round large enough for it to disappear into the per-round
   * cost.
   */
  val fetching: AtomicBoolean
)

private[stream] object Round {

  /**
   * The most elements one claim may reserve.
   *
   * A stride trades load balance against atomic traffic. A worker commits to
   * `stride` elements before it can know whether it will be the round's
   * straggler, so the tail costs up to `(stride - 1) * cost(f)` of idle time
   * for the other workers to save `(stride - 1) / stride` of the cursor's
   * atomic operations. The cap bounds that tail.
   *
   * 64 rather than 16, which is where this was originally set. At 16 the cap was
   * binding hard in the regime it matters most: at 512-element chunks with
   * `n = 4`, a fused round of sixteen chunks wants a stride of `8192 / 32 = 256`,
   * so 16 discarded most of the available amortization.
   *
   * Swept against two benchmarks, because one alone is misleading in each
   * direction. `FetchPathBenchmark` at 512-element chunks with `n = 4` is the
   * uniform, cheap-`f` case the amortization helps; `SkewedCostBenchmark` at
   * `costRatio = 20`, `n = 4` clusters expensive elements so one claim can land
   * entirely on them, which is the tail this cap exists to bound.
   *
   * | `MaxStride` | uniform | skew |
   * |---|---|---|
   * | 16 | 646.41 ± 34.05 | 50.24 ± 1.80 |
   * | 32 | 652.41 ± 54.23 (+0.9%) | 51.54 ± 2.07 (+2.6%) |
   * | '''64''' | '''693.89 ± 26.40 (+7.3%)''' | '''51.82 ± 1.24 (+3.1%)''' |
   * | 256 | 739.14 ± 33.06 (+14.3%) | 44.53 ± 0.71 ('''-11.4%''') |
   *
   * 64 improves '''both''' columns, so it is not a compromise between them. The
   * regression appears only between 64 and 256, which makes this a cliff rather
   * than a gradual trade, and 64 sits below it.
   *
   * ==A cap in elements cannot bound a tail measured in work==
   *
   * This value was briefly 256, on a `FetchPathBenchmark` measurement alone. The
   * check run at the time used `CrossoverBenchmark` at `fCostIters = 5000`, which
   * is '''uniform''' cost: every claim is equally expensive, so no worker can be
   * a straggler and the cap's purpose goes untested. With clustered costs, 256
   * measured -13.6% (51.34 ± 1.63 to 44.36 ± 0.76, fork spreads 1.2% and 0.7%),
   * with all three controls flat.
   *
   * The underlying reason is worth keeping in view. [[ClaimsPerWorker]] bounds
   * the tail at `1 / ClaimsPerWorker` of the round '''in units of work''',
   * whatever `f` costs, because `length / (n * ClaimsPerWorker)` shrinks the
   * stride exactly when a round holds few elements per worker. That bound is
   * independent of `cost(f)`, which is what makes it sound. This cap is an
   * absolute element count, so once it binds it silently replaces that guarantee
   * with "at most `MaxStride` elements, however long those take". At 256
   * clustered slow elements that was roughly 1.5ms serialized behind one worker.
   *
   * So the cap is a backstop for rounds large enough that even a work-proportional
   * bound is a lot of wall-clock, and it has to stay small enough that a claim of
   * pathological elements is still survivable. Raising it further needs the skew
   * benchmark, not just the uniform one.
   */
  private final val MaxStride = 64

  /**
   * How many claims each worker should get per round, at minimum. This is what
   * bounds the tail imbalance in units of *work* rather than elements: with
   * `c` claims apiece, a worker that draws one oversized claim is at most
   * `1 / c` of the round behind, whatever `f` costs.
   *
   * A stride sized to give each worker exactly one claim is what an earlier
   * revision did, and it measured 7-9% *slower* at `n` in the thousands with a
   * 5ms `f`: one claim per worker means the round ends when the slowest single
   * claim ends, so a 16-element claim serialized 80ms behind the others.
   * Requiring several claims apiece keeps the same amortization for a cheap
   * `f` — where the stride is capped by `MaxStride` long before this bites —
   * while restoring fine-grained balance once elements per worker is the
   * binding constraint.
   */
  private final val ClaimsPerWorker = 8

  /**
   * Elements per claim, for a round of `length` elements dispatched to `n`
   * workers.
   *
   * `length / (n * ClaimsPerWorker)` is what preserves the concurrency
   * contract. Batching is sound only while the round still offers at least one
   * claim per worker, and this asks for several: a chunk of `>= n` elements
   * still reaches all `n` workers — the "a single chunk keeps all n workers
   * busy" guarantee — because below `n * ClaimsPerWorker` elements per round
   * the quotient is 0 and `max 1` pins the stride to 1, degrading dispatch to
   * exactly the per-element cursor.
   *
   * Batching therefore engages only where it is both safe and useful: rounds
   * far larger than `n`, which is precisely the regime where per-element
   * atomic traffic on the shared cursor is the bottleneck, and where the tail
   * a stride costs is a vanishing fraction of the round.
   */
  def strideFor(length: Int, n: Int): Int =
    if (length <= 0 || n <= 0) 1
    else ((length / (n.toLong * ClaimsPerWorker)).toInt max 1) min MaxStride

  def data[E, A](chunk: Chunk[A], n: Int): Round[E, A] = {
    val stride = strideFor(chunk.length, n)
    new Round(
      chunk,
      new AtomicInteger(0),
      makePromise[E, A],
      terminal = false,
      stride,
      // Only a batched round elects by CAS; at stride 1 the implicit
      // `i == length` test does it, so the flag is never read and is left
      // unallocated. That keeps a slow-`f` run — where every round is stride 1
      // — allocating exactly what it did before batching existed.
      if (stride == 1) null else new AtomicBoolean(false)
    )
  }

  def terminal[E, A]: Round[E, A] =
    // A terminal round's `next` is never awaited: `loop` checks `terminal`
    // before touching the cursor, so a worker that loops onto a terminal round
    // stops immediately, and a worker awaiting the *previous* round's `next`
    // receives this round and then hits that same check. The promise is
    // therefore never completed and never read; it exists only to fill the
    // field.
    new Round[E, A](
      Chunk.empty,
      new AtomicInteger(0),
      makePromise[E, A],
      terminal = true,
      stride = 1,
      fetching = null
    )

  private def makePromise[E, A]: Promise[Nothing, Round[E, A]] =
    Promise.unsafe.make[Nothing, Round[E, A]](FiberId.None)(Unsafe)
}
