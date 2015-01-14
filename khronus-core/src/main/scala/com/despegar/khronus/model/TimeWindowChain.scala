/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.despegar.khronus.model

import com.despegar.khronus.store.MetaSupport
import com.despegar.khronus.util.Settings
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.despegar.khronus.util.log.Logging

class TimeWindowChain extends TimeWindowsSupport with Logging with MetaSupport {

  def mustExecute(timeWindow: TimeWindow[_, _], metric: Metric, tick: Tick): Boolean = {
    if ((tick.endTimestamp.ms % timeWindow.duration.toMillis) == 0) {
      true
    } else {
      log.debug(s"${p(metric, timeWindow.duration)} Excluded to run")
      false
    }
  }

  def process(metric: Metric): Future[Seq[Any]] = {

    val windows: Seq[TimeWindow[_, _]] = metric.mtype match {
      case MetricType.Timer | MetricType.Gauge ⇒ histrogramsWindows
      case MetricType.Counter                  ⇒ countersWindows
    }

    val tick = currentTick(windows)

    val sequence = Future.sequence(Seq(processInChain(windows filter (mustExecute(_, metric, tick)), metric, tick, 0)))
    sequence onSuccess {
      case _ ⇒ metaStore.update(metric, tick.endTimestamp)
    }
    sequence
  }

  protected def currentTick(windows: Seq[TimeWindow[_, _]]) = Tick.current(windows)

  def processInChain(windows: Seq[TimeWindow[_, _]], metric: Metric, tick: Tick, index: Int): Future[Unit] = {
    if (windows.size > 0) {
      if (index >= (windows.size - 1)) {
        windows(index).process(metric, tick)
      } else {
        windows(index).process(metric, tick).flatMap { _ ⇒
          processInChain(windows, metric, tick, index + 1)
        }
      }
    } else {
      Future {}
    }
  }
}