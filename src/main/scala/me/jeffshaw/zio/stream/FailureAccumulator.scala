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
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicReference

/**
 * Accumulates the causes of a `runForeachPar` run and signals fail-fast.
 *
 * Three things that have to agree are held together here, because they are one
 * mechanism: whether the run failed, why it failed, and when the other workers
 * should stop. The signal decides ''whether''; the accumulated cause holds
 * ''why''. They can legitimately disagree, which is the whole reason this is a
 * type rather than three bindings:
 *
 *   - An interruption-only cause fires the signal but is deliberately '''not'''
 *     recorded, matching `ZChannel#mapOutZIOParUnordered`. Interruption is
 *     normally the ''consequence'' of the failure already being recorded, since
 *     fail-fast interrupts the other workers, so folding it in would bury the
 *     real cause under the interrupts it triggered. The run must still fail, and
 *     it fails with an empty cause.
 *   - So a failed run can carry no recorded cause at all. [[result]] returns
 *     that as `Exit.failCause(Cause.empty)`, which is a failure whose cause is
 *     empty, not a success.
 *
 * ==What this type enforces==
 *
 * The signal and the accumulated cause are `private[this]` and have no
 * accessors, so they can only be reached through the three members below. That
 * makes three invariants structural rather than a matter of remembering them:
 *
 *   - Recording is sequenced '''before''' the signal fires. [[record]] is the
 *     only way to touch either, and it does both in that order, so a cause is
 *     committed by the time anything can observe the signal and interrupt.
 *   - An interruption-only cause is not recorded. There is one branch, in one
 *     place, and no way to record a cause without passing through it.
 *   - The two are read back '''together'''. [[result]] is a single read that
 *     returns both as one [[Exit]], so the "failed, with nothing recorded" case
 *     cannot be misread as success by consulting the cause alone.
 *
 * What it does '''not''' enforce: that recording accumulates rather than
 * replaces. The type cannot express that; it only ensures there is exactly one
 * update site. `RunForeachParSpec`'s "both concurrent failures are reachable in
 * one exit" is what actually guards it. Nor can it compel the dispatcher to
 * invoke [[record]] on every failure path; that stays the caller's contract,
 * documented on [[ChunkCursorDistributor.run]].
 */
private[stream] final class FailureAccumulator[E] private (
  private[this] val errorSignal: Promise[Nothing, Unit],
  private[this] val failure: AtomicReference[Cause[E]]
)(implicit trace: Trace) {

  /**
   * Records a cause and fires the fail-fast signal, in that order.
   *
   * Built once per run and handed to [[ChunkCursorDistributor.run]] as its
   * `onError`, where it is captured as the error continuation of the
   * per-element fold. It must therefore stay a single pre-built function value:
   * a method would allocate a fresh closure at every capture site.
   *
   * `Promise#succeedUnit` is `private[zio]`; it exists only to skip the `Exit`
   * allocation of `succeed(())`. `Exit.unit` is a singleton, so
   * `done(Exit.unit)` is the allocation-free public equivalent. This runs once
   * per failure, not per element.
   */
  val record: Cause[E] => ZIO[Any, Nothing, Unit] =
    (cause: Cause[E]) =>
      (if (cause.isInterruptedOnly) Exit.unit
       else ZIO.succeed(failure.getAndUpdate(_ && cause))) *>
        errorSignal.done(Exit.unit).unit

  /** Completes once the run has failed, so callers can stop the workers. */
  def await: ZIO[Any, Nothing, Unit] =
    errorSignal.await

  /**
   * The run's outcome, read as one value.
   *
   * `Exit.unit` if the run never failed. `Exit.failCause(c)` if it did, where
   * `c` is empty for an interruption-only cause: fired the signal, deliberately
   * not recorded. An empty cause here therefore means "failed with nothing
   * recorded", never "did not fail".
   */
  def result: ZIO[Any, Nothing, Exit[E, Unit]] =
    errorSignal.isDone.map(errored => if (errored) Exit.failCause(failure.get) else Exit.unit)
}

private[stream] object FailureAccumulator {

  def make[E](implicit trace: Trace): ZIO[Any, Nothing, FailureAccumulator[E]] =
    Promise
      .make[Nothing, Unit]
      .map(new FailureAccumulator[E](_, new AtomicReference[Cause[E]](Cause.empty)))
}
