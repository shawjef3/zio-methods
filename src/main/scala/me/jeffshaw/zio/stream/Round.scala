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
 * forward through `next` (round k's promise resolves to round k+1), so a
 * reference to any one round transitively reaches every later round. A worker
 * whose `f` is slow to return still holds the round it claimed from, so
 * without releasing, every chunk pulled since then would stay reachable for as
 * long as that `f` runs, and retention would grow with the stream rather than
 * being bounded by `bufferSize`. Clearing the field once the round can hand
 * out no more elements lets the large payload go while the small round objects
 * stay chained. Those are freed once nothing holds an early round, which is
 * why [[Dispatcher]] does not keep the seed for the whole run.
 */
private[stream] final class Round[A](
  @volatile var chunk: Chunk[A],
  val cursor: AtomicInteger,
  val next: Promise[Nothing, Round[A]],
  val isTerminal: Boolean,
  /**
   * How many contiguous elements one claim reserves. See [[Round.claimSizeFor]];
   * a claim size of `1` claims exactly one element per atomic operation.
   */
  val claimSize: Int,
  /**
   * Elects the round's single designated fetcher when `claimSize > 1`: the first
   * worker to find the cursor at or past the end wins it by CAS.
   * [[Dispatcher]] explains why only batched rounds need it.
   *
   * `null` for a round of single-element claims, where `i == length` elects for free, so the
   * flag is neither allocated nor read. That keeps a slow-`f` run, where every
   * round claims one element at a time, from paying any allocation for batching.
   */
  val fetcherElected: AtomicBoolean
)

private[stream] object Round {

  /**
   * The most elements one claim may reserve.
   *
   * Claim size trades load balance against atomic traffic. A worker commits to
   * `claimSize` elements before it can know whether it will be the round's
   * straggler, so the tail costs up to `(claimSize - 1) * cost(f)` of idle time
   * for the other workers to save `(claimSize - 1) / claimSize` of the cursor's
   * atomic operations. The cap bounds that tail.
   *
   * A cap of 16 binds hard in the regime it matters most: at 512-element chunks
   * with `n = 4`, a fused round of sixteen chunks wants a claim size of
   * `8192 / 32 = 256`, so 16 discards most of the available amortization.
   *
   * Swept against two benchmarks, because one alone is misleading in each
   * direction. `FetchPathBenchmark` at 512-element chunks with `n = 4` is the
   * uniform, cheap-`f` case the amortization helps; `SkewedCostBenchmark` at
   * `costRatio = 20`, `n = 4` clusters expensive elements so one claim can land
   * entirely on them, which is the tail this cap exists to bound.
   *
   * | `MaxClaimSize` | uniform | skew |
   * |---|---|---|
   * | 16 | 646.41 ± 34.05 | 50.24 ± 1.80 |
   * | 32 | 652.41 ± 54.23 (+0.9%) | 51.54 ± 2.07 (+2.6%) |
   * | '''64''' | '''693.89 ± 26.40 (+7.3%)''' | '''51.82 ± 1.24 (+3.1%)''' |
   * | 256 | 739.14 ± 33.06 (+14.3%) | 44.53 ± 0.71 ('''-11.4%''') |
   *
   * 64 improves '''both''' columns, so it is not a compromise between them. The
   * loss appears only between 64 and 256, which makes this a cliff rather than
   * a gradual trade, and 64 sits below it.
   *
   * ==A cap in elements cannot bound a tail measured in work==
   *
   * A uniform-cost benchmark cannot test this cap. `FetchPathBenchmark` favors
   * 256, and `CrossoverBenchmark` at `fCostIters = 5000` is '''uniform''' cost:
   * every claim is equally expensive, so no worker can be a straggler and the
   * cap's purpose goes untested. With clustered costs, 256 measures -13.6%
   * against 16 (44.36 ± 0.76 against 51.34 ± 1.63, fork spreads 0.7% and 1.2%),
   * with all three controls (two uniform, one where the claim size pins to 1)
   * flat.
   *
   * [[ClaimsPerWorker]] bounds the tail at `1 / ClaimsPerWorker` of the round
   * '''in units of work''', whatever `f` costs, because
   * `length / (n * ClaimsPerWorker)` shrinks the claim size exactly when a round
   * holds few elements per worker. That bound is independent of `cost(f)`,
   * which is what makes it sound. This cap is an absolute element count, so
   * once it binds it silently replaces that guarantee with "at most
   * `MaxClaimSize` elements, however long those take". At 256 clustered slow
   * elements that is roughly 1.5ms serialized behind one worker.
   *
   * So the cap is a backstop for rounds large enough that even a work-proportional
   * bound is a lot of wall-clock, and it has to stay small enough that a claim of
   * the slowest elements is still survivable. A larger value has to be measured
   * on the skew benchmark, not just the uniform one.
   */
  private[stream] final val MaxClaimSize = 64

  /**
   * How many claims each worker should get per round, at minimum. This is what
   * bounds the tail imbalance in units of *work* rather than elements: with
   * `c` claims apiece, a worker that draws one oversized claim is at most
   * `1 / c` of the round behind, whatever `f` costs.
   *
   * Sizing claims so that each worker gets exactly one measures 7-9% *slower*
   * at `n` in the thousands with a 5ms `f`: one claim per worker means the
   * round ends when the slowest single claim ends, so a 16-element claim
   * serializes 80ms behind the others. Requiring several claims apiece keeps
   * the same amortization for a cheap `f`, where the claim size is capped by
   * `MaxClaimSize` long before this bites, while keeping fine-grained balance
   * once elements per worker is the binding constraint.
   */
  private final val ClaimsPerWorker = 8

  /**
   * Elements per claim, for a round of `length` elements dispatched to `n`
   * workers.
   *
   * `length / (n * ClaimsPerWorker)` is what preserves the concurrency
   * contract. Batching is sound only while the round still offers at least one
   * claim per worker, and this asks for several: a chunk of `>= n` elements
   * still reaches all `n` workers, the "a single chunk keeps all n workers
   * busy" guarantee, because below `n * ClaimsPerWorker` elements per round
   * the quotient is 0 and `max 1` pins the claim size to 1, degrading dispatch to
   * exactly the per-element cursor.
   *
   * Batching therefore engages only where it is both safe and useful: rounds
   * far larger than `n`, which is precisely the regime where per-element
   * atomic traffic on the shared cursor is the bottleneck, and where the tail
   * a larger claim costs is a vanishing fraction of the round.
   *
   * `n` is at least 1: `runForeachPar` hands a non-positive `n` to
   * `runForeach` before any round exists.
   */
  private def claimSizeFor(length: Int, n: Int): Int =
    ((length / (n.toLong * ClaimsPerWorker)).toInt max 1) min MaxClaimSize

  def data[A](chunk: Chunk[A], n: Int): Round[A] = {
    val claimSize = claimSizeFor(chunk.length, n)
    new Round(
      chunk,
      new AtomicInteger(0),
      makePromise[A],
      isTerminal = false,
      claimSize,
      if (claimSize == 1) null else new AtomicBoolean(false)
    )
  }

  /**
   * A stop signal. `loop` checks `isTerminal` before touching anything else, so
   * the cursor and promise are never read and are left null: a change that did
   * read them would fail loudly rather than wait on a promise nobody completes.
   */
  def terminal[A]: Round[A] =
    new Round[A](
      Chunk.empty,
      null,
      null,
      isTerminal = true,
      claimSize = 1,
      fetcherElected = null
    )

  private def makePromise[A]: Promise[Nothing, Round[A]] =
    Promise.unsafe.make[Nothing, Round[A]](FiberId.None)(Unsafe)
}
