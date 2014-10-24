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

import java.util.concurrent.TimeUnit
import com.despegar.metrik.store.MetaSupport
import com.despegar.metrik.util.{ BucketUtils, Logging, Settings }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

//remove this

class TimeWindowChain extends Logging with MetaSupport {

  val histrogramsWindows = Seq(HistogramTimeWindow(30 seconds, 1 millis), HistogramTimeWindow(1 minute, 30 seconds), HistogramTimeWindow(5 minute, 1 minute), HistogramTimeWindow(10 minute, 5 minute, false))

  val countersWindows = Seq(CounterTimeWindow(30 seconds, 1 millis, false))

  def process(metric: Metric): Future[Seq[Any]] = {
    val executionTimestamp = System.currentTimeMillis() - Settings().Window.ExecutionDelay
    log.debug(s"Processing windows for $metric")

    val windows: Seq[TimeWindow] = metric.mtype match {
      case "timer"   ⇒ histrogramsWindows
      case "counter" ⇒ countersWindows
    }

    val sequence = Future.sequence(Seq(processInChain(windows, metric, executionTimestamp, 0)))
    sequence onSuccess {
      case _ ⇒ metaStore.update(metric, BucketUtils.getCurrentBucketTimestamp(windows(0).duration, executionTimestamp))
    }
    sequence
  }

  def processInChain(windows: Seq[TimeWindow], metric: Metric, executionTimestamp: Long, index: Int): Future[Unit] = {
    if (index >= (windows.size - 1)) {
      windows(index).process(metric, executionTimestamp)
    } else {
      windows(index).process(metric, executionTimestamp).flatMap { _ ⇒
        processInChain(windows, metric, executionTimestamp, index + 1)
      }
    }
  }

}