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
import com.despegar.metrik.model.CounterBucket._
import com.despegar.metrik.store._
import com.despegar.metrik.util.{ BucketUtils, Logging }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

abstract class TimeWindow[T <: Bucket, U <: Summary] extends BucketStoreSupport[T] with SummaryStoreSupport[U] with MetaSupport with Logging {

  def process(metric: Metric, executionTimestamp: Long): Future[Unit] = {
    log.debug(s"Process HistogramTimeWindow of $duration for metric $metric")
    //retrieve the temporal histogram buckets from previous window
    val previousWindowBuckets = retrievePreviousBuckets(metric, executionTimestamp)

    //group histograms in buckets of my window duration
    val groupedHistogramBuckets = groupInBucketsOfMyWindow(previousWindowBuckets, metric)

    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val filteredGroupedHistogramBuckets = filterAlreadyProcessedBuckets(groupedHistogramBuckets, metric)

    val resultingBuckets = aggregateBuckets(filteredGroupedHistogramBuckets)

    //store temporal histogram buckets for next window if needed
    val storeTemporalFuture = storeTemporalBuckets(resultingBuckets, metric)

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = storeTemporalFuture flatMap { _ ⇒ resultingBuckets.map(buckets ⇒ buckets map (getSummary(_))) }

    //store the statistic summaries
    val storeFuture = statisticsSummaries flatMap (summaries ⇒ summaryStore.store(metric, duration, summaries))

    //remove previous histogram buckets
    storeFuture flatMap { _ ⇒ previousWindowBuckets flatMap (windows ⇒ bucketStore.remove(metric, previousWindowDuration, windows)) }
  }

  protected def getSummary(bucket: T): U

  private def storeTemporalBuckets(resultingBuckets: Future[Seq[T]], metric: Metric) = {
    if (shouldStoreTemporalHistograms) {
      resultingBuckets flatMap (buckets ⇒ bucketStore.store(metric, duration, buckets)) andThen {
        case Failure(reason) ⇒ log.error("Fail to store temporal buckets", reason)
        case Success(_)      ⇒ log.info("Success to store temporal buckets")
      }
    } else {
      Future.successful[Unit](log.debug("Last window. No need to store buckets"))
    }
  }

  //  protected def aggregateBuckets(buckets: Future[Map[Long, scala.Seq[Bucket]]]): Future[Seq[Bucket]]

  protected def aggregateBuckets(buckets: Future[Map[Long, Seq[T]]]): Future[Seq[T]] = {
    buckets map (buckets ⇒ buckets.collect { case (bucketNumber, buckets) ⇒ aggregate(bucketNumber, buckets) }.toSeq)
  }

  protected def aggregate(bucketNumber: Long, buckets: Seq[T]): T

  def duration: Duration

  protected def previousWindowDuration: Duration

  protected def shouldStoreTemporalHistograms: Boolean

  private def retrievePreviousBuckets(metric: Metric, executionTimestamp: Long) = {
    bucketStore.sliceUntil(metric, BucketUtils.getCurrentBucketTimestamp(duration, executionTimestamp), previousWindowDuration)
  }

  private def groupInBucketsOfMyWindow(previousWindowBuckets: Future[Seq[T]], metric: Metric) = {
    previousWindowBuckets map (_.groupBy(_.timestamp / duration.toMillis)) andThen {
      case Success(buckets) ⇒
        log.debug(s"${buckets.size} grouped buckets: ${buckets} of $duration for metric $metric")
    }
  }

  private def filterAlreadyProcessedBuckets(groupedHistogramBuckets: Future[Map[Long, Seq[T]]], metric: Metric) = {
    lastProcessedBucket(metric) flatMap { lastBucket ⇒ groupedHistogramBuckets map (_.filterNot(_._1 < (lastBucket - 1))) } andThen {
      case Success(buckets) ⇒
        log.debug(s"${buckets.size} buckets after filtering: ${buckets} of $duration for metric $metric")
    }
  }

  /**
   * Returns the last bucket number found in statistics summaries
   * @param metric
   * @return a Long representing the bucket number. If nothing if found -1 is returned
   */
  private def lastProcessedBucket(metric: Metric): Future[Long] = {
    metaStore.getLastProcessedTimestamp(metric) map { timestamp ⇒ timestamp / duration.toMillis } andThen {
      case Success(bucket) ⇒
        log.debug(s"Last processed bucket: $bucket of $duration for metric $metric")
    }
  }

}

case class CounterTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[CounterBucket, CounterSummary] with CounterBucketStoreSupport with CounterSummaryStoreSupport {

  override def aggregate(bucketNumber: Long, buckets: Seq[CounterBucket]): CounterBucket = new CounterBucket(bucketNumber, duration, buckets)

  override def getSummary(bucket: CounterBucket): CounterSummary = bucket.summary

}

case class HistogramTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[HistogramBucket, StatisticSummary] with HistogramBucketSupport with StatisticSummarySupport {

  override def aggregate(bucketNumber: Long, buckets: Seq[HistogramBucket]): HistogramBucket = new HistogramBucket(bucketNumber, duration, buckets)

  override def getSummary(bucket: HistogramBucket): StatisticSummary = bucket.summary
}