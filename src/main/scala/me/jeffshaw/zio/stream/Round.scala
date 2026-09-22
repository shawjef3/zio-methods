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
   * 256 rather than 16, which is where this was originally set. The cap was
   * binding by a factor of 16 in the regime it matters most: at 512-element
   * chunks with `n = 4`, a fused round of sixteen chunks wants a stride of
   * `8192 / 32 = 256`, and 16 threw away fifteen sixteenths of the available
   * amortization.
   *
   * Swept over {16, 64, 256} on `FetchPathBenchmark`, then the endpoints
   * re-measured at `-f 5 -wi 10 -i 10`:
   *
   *   - 512-element chunks, `n = 4`, no-op `f`: 698.87 ± 8.97 to 788.25 ± 8.67
   *     ops/s, '''+12.8%''', fork spreads 1.2% and 2.9%.
   *   - 64-element chunks, `n = 4`: -1.0% at 64 and -1.8% at 256, both inside
   *     error. Rounds there are too small for the cap to bind, so this is the
   *     control that should not move, and does not.
   *   - 512-element chunks, `n = 64`: the stride is 1 whatever the cap is, so
   *     this cannot legitimately move either. Its apparent +7.6% came with a
   *     20% fork spread and is noise.
   *
   * The tail the cap exists to bound was measured too, since a no-op `f` cannot
   * show it. `CrossoverBenchmark` at `fCostIters = 5000`, `n = 4`, expensive
   * enough that a 256-element claim is a real serialization risk, reads
   * 62.51 ± 2.89 against 61.57 ± 2.73, i.e. -1.5% with heavily overlapping
   * bars. [[ClaimsPerWorker]] is what actually protects that case: it keeps the
   * quotient at 1 whenever a round holds fewer than `n * 8` elements, which is
   * the norm once `f` is slow, so the cap is not reached there at all.
   */
  private final val MaxStride = 256

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
