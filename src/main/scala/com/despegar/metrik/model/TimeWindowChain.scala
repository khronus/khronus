/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.model

import com.despegar.metrik.store.MetaSupport
import com.despegar.metrik.util.{ Logging, Settings }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TimeWindowChain extends TimeWindowsSupport with Logging with MetaSupport {

  def mustExecute(timeWindow: TimeWindow[_, _], metric: Metric, timestamp: Timestamp): Boolean = {
    if ((timestamp.ms % timeWindow.duration.toMillis) == 0) {
      true
    } else {
      log.debug(s"${p(metric, timeWindow.duration)} Exclude to run")
      false
    }
  }

  def now = System.currentTimeMillis()

  def process(metric: Metric): Future[Seq[Any]] = {
    val executionTimestamp = Timestamp(now - Settings().Window.ExecutionDelay)
    log.debug(s"Processing windows for $metric on executionTimestamp ${executionTimestamp.ms}")

    val windows: Seq[TimeWindow[_, _]] = metric.mtype match {
      case "timer"   ⇒ histrogramsWindows
      case "counter" ⇒ countersWindows
    }

    val timestampAligned = executionTimestamp.alignedTo(firstDuration(windows))
    log.debug(s"Execution timestamp aligned to $timestampAligned")

    val sequence = Future.sequence(Seq(processInChain( windows filter (mustExecute(_, metric, timestampAligned)), metric, executionTimestamp, 0)))
    sequence onSuccess {
      case _ ⇒ metaStore.update(metric, executionTimestamp.alignedTo(firstDuration(windows)))
    }
    sequence
  }

  private def firstDuration(windows: Seq[TimeWindow[_, _]]) = windows(0).duration

  def processInChain(windows: Seq[TimeWindow[_, _]], metric: Metric, executionTimestamp: Timestamp, index: Int): Future[Unit] = {
    if (windows.size > 0) {
      if (index >= (windows.size - 1)) {
        windows(index).process(metric, executionTimestamp)
      } else {
        windows(index).process(metric, executionTimestamp).flatMap { _ ⇒
          processInChain(windows, metric, executionTimestamp, index + 1)
        }
      }
    } else {
      Future {}
    }
  }

}