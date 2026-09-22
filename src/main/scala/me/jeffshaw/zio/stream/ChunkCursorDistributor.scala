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
 * A [[Round]] holds the current chunk, an [[AtomicInteger]] cursor, a `stride`,
 * and a `Promise` for the next round. A worker reads the current round and
 * claims a contiguous range of elements with `i = cursor.getAndAdd(stride)`:
 *
 *   - `i < chunk.length`: run `f` over `[i, min(i + stride, length))`, one
 *     element after another without returning to the cursor, then loop on the
 *     same round.
 *   - `i >= chunk.length`: the round is exhausted, and exactly one worker here is
 *     elected the ''designated fetcher''. It pulls the next [[Take]] from `fetch`
 *     and publishes the resulting round via `next`. For a data round it then
 *     loops on it; for a terminal round it returns instead (reporting the cause
 *     first, if the terminal is a failure), so it never re-observes the terminal
 *     it just published.
 *   - `i >= chunk.length` and not elected: another worker is fetching; await
 *     `next`, then loop on the round it published.
 *
 * ==Stride==
 *
 * A stride above 1 amortizes the cursor's atomic operation over several
 * elements, which is what the dispatch loop's cost is dominated by once `f` is
 * cheap. It is derived per round from `length / (n * ClaimsPerWorker)`, so it
 * engages only for rounds far larger than `n` and always leaves every worker
 * several claims — the load balance a single shared cursor exists to provide is
 * preserved, and a chunk of `>= n` elements still reaches all `n` workers.
 * Below that threshold the stride is 1 and dispatch is exactly per-element.
 *
 * The stride also decides how the fetcher is elected. At stride 1 the bases are
 * consecutive, so exactly one worker sees `i == length` and that test elects it
 * with no extra atomic — the original protocol, unchanged. A larger stride makes
 * the bases skip, so none need land on `length` at all and the same test would
 * elect nobody and hang the run; those rounds elect by CAS on `fetching`
 * instead, which costs one atomic per round on rounds that are by construction
 * large.
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
 * are linearized: the claimed ranges partition `[0, length)`, so no index is
 * handed out twice and none is skipped.
 */
private[stream] object ChunkCursorDistributor {

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
   *
   * ==Starting the workers: a precondition, not an implementation detail==
   *
   * All `n` workers begin on the shared [[Dispatcher.seed]] round, which is a
   * '''single-use election point''': it is already exhausted, so the one worker
   * whose claim returns `i == 0` becomes the initial fetcher and every other
   * worker awaits `seed.next`. That only holds while the seed is alive. Once
   * the elected fetcher publishes and `release`s it, a worker arriving later
   * finds a round it can neither claim from nor be elected on, and the run
   * degenerates: each late arrival starts its own independent fetch/dispatch
   * sequence instead of joining the shared one.
   *
   * So `foreachParDiscard` here is '''load-bearing''', not an arbitrary way to
   * spell "run these `n` effects". Replacing it with a fork loop was attempted
   * and reverted; four variants (`forkIn` inside `ZIO.scopedWith`, `fork`,
   * `forkDaemon`, and a fork loop under `uninterruptibleMask`) each broke
   * 35-50 of the 78 tests, with this signature:
   *
   *   - `fetch is invoked exactly once per round` at `n = 2`: 3 calls, not 2.
   *   - `stops pulling once a terminal round is reached` at `n = 32`: 33, not 2.
   *   - `a failure terminal is reported exactly once`: reported twice.
   *   - `a single chunk keeps all n workers busy`: times out.
   *
   * `n + 1` fetches is the tell: every worker elected itself. The tests above
   * are the regression guard, and they fail loudly rather than subtly, so the
   * protocol is not silently at risk — but they diagnose the symptom, not the
   * cause, which is why it is written down here.
   *
   * '''What exactly `foreachParDiscard` provides is not established.''' It is
   * something about how ZIO's `foreachParUnboundedDiscard` (the branch taken
   * here, since `parallelism == size`) schedules the forked children relative
   * to the forking fiber; `uninterruptibleMask` alone does not reproduce it,
   * and made matters worse. Anyone reworking this should not try to guess it.
   *
   * The robust fix, if this call ever needs to change — for instance to drop
   * the `Chunk` of `n` `Fiber.Runtime`s that `foreachParUnboundedDiscard`
   * retains for the whole run — is to '''remove the dependency''' rather than
   * reproduce it: elect the initial fetcher by CAS on a dedicated flag instead
   * of relying on `i == 0` being unique among workers that may arrive at
   * different times. That makes startup order irrelevant, after which the
   * worker-forking strategy is free.
   */
  def run[R, E <: E1, E1, A](
    n: Int,
    fetch: ZIO[R, Nothing, Take[E, A]],
    f: A => ZIO[R, E1, Any],
    onError: Cause[E1] => ZIO[R, Nothing, Unit]
  )(implicit trace: Trace): ZIO[R, Nothing, Unit] =
    ZIO.suspendSucceed {
      // One dispatcher per run, shared by all `n` workers. Its fields are the
      // state every step of the loop needs and none of it changes during the
      // run, so holding them here keeps them off the recursive calls: as nested
      // defs, `n`/`fetch`/`f`/`onError`/`trace` were lifted into every call's
      // argument list, including the per-element ones.
      val dispatcher = new Dispatcher[R, E, E1, A](n, fetch, f, onError)
      // Load-bearing: the workers share a single-use election point, so this
      // cannot be swapped for a fork loop without first making seed election
      // tolerate late arrivals. See the precondition on this method.
      ZIO.foreachParDiscard(1 to n)(_ => dispatcher.loop(dispatcher.seed, 0)).withParallelism(n)
    }
}
