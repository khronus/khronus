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
    log.debug(s"Process window of $duration for metric $metric")
    //retrieve the temporal histogram buckets from previous window
    val previousWindowBuckets = histogramBucketStore.sliceUntil(metric, BucketUtils.getCurrentBucketTimestamp(duration, executionTimestamp), previousWindowDuration)

    //group histograms in buckets of my window duration
    val groupedHistogramBuckets = groupInBucketsOfMyWindow(previousWindowBuckets, metric)

    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val filteredGroupedHistogramBuckets = filterAlreadyProcessedBuckets(groupedHistogramBuckets, metric)

    //sum histograms on each bucket
    val resultingBuckets = filteredGroupedHistogramBuckets map (buckets ⇒ buckets.collect { case (bucketNumber, histogramBuckets) ⇒ HistogramBucket(bucketNumber, duration, histogramBuckets) }.toSeq)

    //store temporal histogram buckets for next window if needed
    val storeTemporalFuture = if (shouldStoreTemporalHistograms) {
      resultingBuckets flatMap (buckets ⇒ histogramBucketStore.store(metric, duration, buckets))
    } else {
      Future.successful[Unit](log.debug("Last window. No need to store buckets"))
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = storeTemporalFuture flatMap { _ ⇒ resultingBuckets.map(buckets ⇒ buckets map (_.summary)) }

    //store the statistic summaries
    val storeFuture = statisticsSummaries flatMap (summaries ⇒ statisticSummaryStore.store(metric, duration, summaries))

    //remove previous histogram buckets
    storeFuture flatMap { _ ⇒ previousWindowBuckets flatMap (windows ⇒ histogramBucketStore.remove(metric, previousWindowDuration, windows)) }
  }

  private def groupInBucketsOfMyWindow(previousWindowBuckets: Future[Seq[HistogramBucket]], metric: String) = {
    val future = previousWindowBuckets map (buckets ⇒ buckets.groupBy(_.timestamp / duration.toMillis))
    future.onSuccess { case buckets ⇒ log.debug(s"${buckets.size} grouped buckets: ${buckets} of $duration for metric $metric") }
    future
  }

  private def filterAlreadyProcessedBuckets(groupedHistogramBuckets: Future[Map[Long, Seq[HistogramBucket]]], metric: String) = {
    val future = lastProcessedBucket(metric) flatMap { lastBucket ⇒ groupedHistogramBuckets map (_.filterNot(_._1 < (lastBucket - 1))) }
    future.onSuccess { case buckets ⇒ log.debug(s"${buckets.size} buckets after filtering: ${buckets} of $duration for metric $metric") }
    future
  }

  /**
   * Returns the last bucket number found in statistics summaries
   * @param metric
   * @return a Long representing the bucket number. If nothing if found -1 is returned
   */
  private def lastProcessedBucket(metric: String): Future[Long] = {
    val future = metaStore.getLastProcessedTimestamp(metric) map { timestamp ⇒ timestamp / duration.toMillis }
    future.onSuccess { case bucket ⇒ log.debug(s"Last processed bucket: $bucket of $duration for metric $metric") }
    future
  }
}