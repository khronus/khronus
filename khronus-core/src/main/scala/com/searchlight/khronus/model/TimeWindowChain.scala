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

package com.searchlight.khronus.model

import com.searchlight.khronus.store.MetaSupport
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ Measurable, FutureSupport, SameThreadExecutionContext }
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }

class TimeWindowChain extends TimeWindowsSupport with Logging with MetaSupport with FutureSupport with Measurable {

  private implicit val executionContext = SameThreadExecutionContext

  def process(metrics: Seq[Metric])(implicit clock: Clock = SystemClock): Future[Unit] = {
    val tick = Tick()
    sequenced(metrics) { metric ⇒
      process(metric, tick)
    } flatMap { metrics ⇒
      updateTickProcessed(tick, metrics)
    }
  }

  private def updateTickProcessed(tick: Tick, metrics: Seq[Try[Metric]]): Future[Unit] = metaStore.update(successful(metrics), tick.endTimestamp)

  private def successful(metrics: Seq[Try[Metric]]): Seq[Metric] = metrics.collect { case Success(m) ⇒ m }

  private def process(metric: Metric, currentTick: Tick): Future[Try[Metric]] = {
    windowsToBeProcessed(metric, currentTick) flatMap { windows ⇒
      sequenced(windows) { window ⇒
        window.process(metric, currentTick) andThen inCaseOfFailure(window, metric)
      } flatMap (f ⇒ Future.successful(Success(metric))) recover { case reason ⇒ Failure(reason) }
    }
  }

  private def inCaseOfFailure(window: Window, metric: Metric): PartialFunction[Try[Unit], Unit] = {
    case Failure(reason) ⇒ {
      incrementCounter("timeWindowError")
      log.error(s"Fail to process window ${window.duration} for $metric", reason)
    }
  }

  private def windowsToBeProcessed(metric: Metric, currentTick: Tick): Future[Seq[Window]] = {
    metaStore.getLastProcessedTimestamp(metric).map { lastProcessed ⇒
      windows(metric.mtype).filter { window ⇒ mustExecuteInThisTick(window, lastProcessed, currentTick) }
    }
  }

  private def mustExecuteInThisTick(window: Window, lastProcessed: Timestamp, tick: Tick): Boolean = {
    val lastProcessedBucket = lastProcessed.fromEndTimestampToBucketNumberOf(window.duration)
    val currentBucket = tick.endTimestamp.fromEndTimestampToBucketNumberOf(window.duration)
    currentBucket > lastProcessedBucket
  }

}