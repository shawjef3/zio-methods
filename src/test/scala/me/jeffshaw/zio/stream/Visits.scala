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
import zio.test._

/** Counts how often each element is visited, for the "exactly once" tests. */
private[stream] object Visits {

  /** Counts one visit of `a`. */
  def record(counts: Ref[Map[Int, Int]], a: Int): UIO[Unit] =
    counts.update(m => m.updated(a, m.getOrElse(a, 0) + 1))

  /** That `total` distinct elements were each visited exactly once. */
  def eachOnce(visits: Map[Int, Int], total: Int): TestResult =
    assertTrue(visits.size == total) && assertTrue(visits.values.forall(_ == 1))
}
