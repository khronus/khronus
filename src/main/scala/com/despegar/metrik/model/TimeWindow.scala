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

import com.despegar.metrik.model.HistogramBucket._
import com.despegar.metrik.store._
import com.despegar.metrik.util.{ BucketUtils, Logging }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

case class TimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends HistogramBucketSupport with StatisticSummarySupport with MetaSupport with Logging {

  def process(metric: String, executionTimestamp: Long): Future[Unit] = {
    log.debug(s"Processing window of $duration for metric $metric and executionTimestamp $executionTimestamp")
    //retrieve the temporal histogram buckets from previous window until the last complete current window
    //Ex. 100.000 (executionTimestamp) / 30.000 (duration) = 90.000 (currentBucketTimestamp)
    val previousWindowBuckets: Future[Seq[HistogramBucket]] = histogramBucketStore.sliceUntil(metric, BucketUtils.getCurrentBucketTimestamp(duration, executionTimestamp), previousWindowDuration)

    //group histograms in buckets of my window duration
    def groupHistogramBuckets: Future[Map[Long, Seq[HistogramBucket]]] = previousWindowBuckets.map {
      buckets ⇒
        buckets.groupBy(_.timestamp / duration.toMillis)
    }

    def filter(buckets: Map[Long, Seq[HistogramBucket]], filterBy: Long): Future[Map[Long, Seq[HistogramBucket]]] = Future {
      buckets.filterNot(_._1 <= filterBy)
    }

    //sum histograms on each bucket
    def sumAndCollectBuckets(buckets: Map[Long, Seq[HistogramBucket]]): Future[Seq[HistogramBucket]] = Future {
      buckets.collect {
        case (bucketNumber, histogramBuckets) ⇒ HistogramBucket(bucketNumber, duration, histogramBuckets)
      }.toSeq
    }

    def calculateStatisticsSummaries(buckets: Seq[HistogramBucket]): Future[Seq[StatisticSummary]] = Future {
      buckets.map {
        bucket ⇒
          bucket.summary
      }
    }

    def storeStatisticsSummaries(summaries: Seq[StatisticSummary]) = Future {
      ifNotEmpty(summaries) {
        statisticSummaryStore.store(metric, duration, summaries)
      }
    }

    def removeHistogram(buckets: Future[Seq[HistogramBucket]]): Future[Unit] = Future {
      buckets.flatMap {
        windows ⇒
          ifNotEmpty(windows) {
            histogramBucketStore.remove(metric, previousWindowDuration, windows)
          }
      }
    }

    val removeTemporalHistograms = Promise[Unit]()

    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val resultingBuckets: Future[Seq[HistogramBucket]] = for {
      lastBucketNumber ← getLastProcessedBucketNumber(metric)
      groupedHistograms ← groupHistogramBuckets
      filteredHistograms ← filter(groupedHistograms, lastBucketNumber)
      collectedHistograms ← sumAndCollectBuckets(filteredHistograms)
    } yield {
      collectedHistograms
    }

    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      resultingBuckets map {
        buckets ⇒
          ifNotEmpty(buckets) {
            histogramBucketStore.store(metric, duration, buckets)
          }
      }
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val summaries: Future[Unit] = for {
      buckets ← resultingBuckets
      statisticsSummaries ← calculateStatisticsSummaries(buckets)
      storedSummaries ← storeStatisticsSummaries(statisticsSummaries)
    } yield {
      storedSummaries
    }

    summaries onComplete {
      case Success(_) ⇒ removeTemporalHistograms.completeWith(removeHistogram(previousWindowBuckets))
      case Failure(NonFatal(e)) ⇒
        log.error("error", e)
        removeTemporalHistograms.failure(e)
    }

    removeTemporalHistograms.future
  }

  /**
   * Call the function f only if the collection is not empty
   */
  def ifNotEmpty(col: Seq[AnyRef])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future {}
    }
  }

  /**
   * Returns the last bucket number found in statistics summaries
   * @param metric
   * @return a Long representing the bucket number. If nothing if found -1 is returned
   */
  private def getLastProcessedBucketNumber(metric: String): Future[Long] = {
    metaStore.getLastProcessedTimestamp(metric) map { timestamp ⇒ timestamp / duration.toMillis }
  }
}