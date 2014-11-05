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

  def mustExecute(timeWindow: TimeWindow[_,_], metric: Metric, timestampAligned: Long): Boolean = {
    if ((timestampAligned % timeWindow.duration.toMillis) == 0) {
      true
    }

    false
  }

  def process(metric: Metric): Future[Seq[Any]] = {
    val executionTimestamp = Timestamp(System.currentTimeMillis() - Settings().Window.ExecutionDelay)
    log.debug(s"Processing windows for $metric")

    val windows: Seq[TimeWindow[_, _]] = metric.mtype match {
      case "timer"   ⇒ histrogramsWindows
      case "counter" ⇒ countersWindows
    }

    val sequence = Future.sequence(Seq(processInChain(windows, metric, executionTimestamp, 0)))
    sequence onSuccess {
      case _ ⇒ metaStore.update(metric, executionTimestamp.alignedTo(firstDuration(windows)))
    }
    sequence
  }

  private def firstDuration(windows: Seq[TimeWindow[_, _]]) = windows(0).duration

  def processInChain(windows: Seq[TimeWindow[_, _]], metric: Metric, executionTimestamp: Long, index: Int): Future[Unit] = {
    if (windows.size > 0 ) {
      if (index >= (windows.size - 1)) {
        windows(index).process(metric, executionTimestamp)
      } else {
        windows(index).process(metric, executionTimestamp).flatMap { _ ⇒
          processInChain(windows, metric, executionTimestamp, index + 1)
        }
      }
    } else {
      Future{}
    }
  }

}