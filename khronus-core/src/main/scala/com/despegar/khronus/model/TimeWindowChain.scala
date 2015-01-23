/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ ConcurrencySupport, SameThreadExecutionContext }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Success

class TimeWindowChain extends TimeWindowsSupport with Logging with MetaSupport {

  import com.despegar.khronus.model.TimeWindowChain._

  implicit val executionContext = SameThreadExecutionContext

  def mustExecute(timeWindow: TimeWindow[_, _], metric: Metric, tick: Tick): Boolean = {
    if (tick.mustExecute(timeWindow)) {
      true
    } else {
      log.debug(s"${p(metric, timeWindow.duration)} Excluded to run")
      false
    }
  }

  def process(metrics: Seq[Metric]): Future[Unit] = Future {
    val tick = currentTick()
    metrics.foldLeft(Future.successful(())) { (previousMetricFuture, metric) ⇒
      previousMetricFuture.flatMap { _ ⇒
        process(metric, tick)
      }
    }
  }(timeWindowExecutionContext) flatMap identity

  def currentTick(): Tick = {
    Tick()
  }

  def process(metric: Metric, currentTick: Tick): Future[Unit] = {
    //TODO: please refactor me
    val windows: Seq[TimeWindow[_, _]] = if (metric.mtype == "counter") countersWindows else histrogramsWindows

    windows.filter(mustExecute(_, metric, currentTick)).foldLeft(Future.successful[Unit](())) { (previousWindow, timeWindow) ⇒
      previousWindow.flatMap(_ ⇒ timeWindow.process(metric, currentTick))
    }.andThen {
      case Success(_) ⇒ metaStore.update(metric, currentTick.endTimestamp)
    }

  }

}

object TimeWindowChain extends ConcurrencySupport {
  val timeWindowExecutionContext: ExecutionContext = executionContext("time-window-worker")

}