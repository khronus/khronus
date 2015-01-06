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

import com.despegar.khronus.model.CounterBucket._
import com.despegar.khronus.model.HistogramBucket._
import com.despegar.khronus.store._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ ConcurrencySupport, Measurable }

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

abstract class TimeWindow[T <: Bucket, U <: Summary] extends BucketStoreSupport[T] with SummaryStoreSupport[U] with MetaSupport with Logging with Measurable {

  import TimeWindow._

  def process(metric: Metric, tick: Tick): scala.concurrent.Future[Unit] = measureFutureTime("processWindow", metric, duration) {
    log.debug(s"${p(metric, duration)} - Processing time window for ${Tick(tick.bucketNumber ~ duration)}")
    //get the last bucket processed for this window
    val lastProcessed = lastProcessedBucket(metric)

    //retrieve the temporal histogram buckets from previous window
    val previousWindowBuckets = retrievePreviousBuckets(metric, tick, lastProcessed)

    //group histograms in buckets of my window duration
    val groupedHistogramBuckets = groupInBucketsOfMyWindow(previousWindowBuckets, metric)

    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val filteredGroupedHistogramBuckets = filterOutAlreadyProcessedBuckets(groupedHistogramBuckets, metric, lastProcessed)

    val resultingBuckets = aggregateBuckets(filteredGroupedHistogramBuckets, metric)

    //store temporal histogram buckets for next window if needed
    val storeTemporalFuture = storeTemporalBuckets(resultingBuckets, metric)

    //calculate the summaries
    val statisticsSummaries = storeTemporalFuture flatMap { _ ⇒ resultingBuckets.map(buckets ⇒ buckets map (getSummary(_))) }

    //store the summaries
    val storeFuture = statisticsSummaries flatMap (summaries ⇒ summaryStore.store(metric, duration, summaries))

    storeFuture
  }

  protected def getSummary(bucket: T): U

  private def storeTemporalBuckets(resultingBuckets: Future[Seq[T]], metric: Metric) = {
    if (shouldStoreTemporalHistograms) measureFutureTime("storeTemporalBuckets", metric, duration) {
      resultingBuckets flatMap { buckets ⇒
        bucketStore.store(metric, duration, buckets) andThen {
          case Failure(reason) ⇒ log.error(s"${p(metric, duration)} - Fail to store temporal buckets", reason)
        }
      }
    }
    else {
      Future.successful[Unit](log.debug(s"${p(metric, duration)} - Last window. No need to store buckets"))
    }
  }

  protected def aggregateBuckets(buckets: Future[Map[BucketNumber, Seq[T]]], metric: Metric): Future[Seq[T]] = measureTime("aggregate", metric, duration) {
    buckets map (buckets ⇒ buckets.collect { case (bucketNumber, buckets) ⇒ aggregate(bucketNumber, buckets) }.toSeq)
  }

  protected def aggregate(bucketNumber: BucketNumber, buckets: Seq[T]): T

  def duration: Duration

  protected def previousWindowDuration: Duration

  protected def shouldStoreTemporalHistograms: Boolean

  private def retrievePreviousBuckets(metric: Metric, tick: Tick, lastProcessed: Future[BucketNumber]) = {
    lastProcessed flatMap (bucketNumber ⇒ {
      val start = System.currentTimeMillis()
      bucketStore.slice(metric, bucketNumber.startTimestamp(), tick.endTimestamp.alignedTo(duration) - 1, previousWindowDuration) andThen {
        case Success(previousBuckets) ⇒
          if (previousBuckets.isEmpty) {
            recordTime(formatLabel("emptySliceTime", metric, duration), System.currentTimeMillis() - start)
          }
          log.debug(s"${p(metric, duration)} - Found ${previousBuckets.size} buckets of $previousWindowDuration")
      }
    })

  }

  private def groupInBucketsOfMyWindow(previousWindowBuckets: Future[Seq[(Timestamp, () ⇒ T)]], metric: Metric): Future[Map[BucketNumber, Seq[T]]] = {
    previousWindowBuckets map (buckets ⇒ buckets.groupBy(tuple ⇒ tuple._1.toBucketNumber(duration)).mapValues(
      seq ⇒ seq.view.map(t ⇒ t._2()))) andThen {
      case Success(buckets) ⇒
        if (!buckets.isEmpty) {
          log.debug(s"${p(metric, duration)} - Grouped ${buckets.size} buckets ${buckets.keys}")
        }
    }
  }

  private def filterOutAlreadyProcessedBuckets(groupedHistogramBuckets: Future[Map[BucketNumber, Seq[T]]], metric: Metric, lastProcessed: Future[BucketNumber]) = {
    lastProcessed flatMap { lastBucket ⇒
      groupedHistogramBuckets map { groups ⇒
        (groups, groups.filter(_._1.number > lastBucket.number))
      }
    } andThen {
      case Success((groups, filteredBuckets)) ⇒ {
        val filteredCount = groups.size - filteredBuckets.size
        if (filteredCount > 0) {
          log.debug(s"${p(metric, duration)} - Filtered out ${filteredCount} already processed buckets")
        }
      }
    } map {
      _._2
    }
  }

  private def lastProcessedBucket(metric: Metric): Future[BucketNumber] = {
    metaStore.getLastProcessedTimestamp(metric) map { lastTS ⇒ Timestamp(lastTS.ms - duration.toMillis).alignedTo(duration).toBucketNumber(duration) } andThen {
      case Success(bucket) ⇒
        log.debug(s"${p(metric, duration)} - Last processed bucket: $bucket")
    }
  }

}

object TimeWindow extends ConcurrencySupport {
  implicit val executionContext: ExecutionContext = executionContext("time-window-worker")
}

case class CounterTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[CounterBucket, CounterSummary] with CounterBucketStoreSupport with CounterSummaryStoreSupport {

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[CounterBucket]): CounterBucket = new CounterBucket(bucketNumber, buckets)

  override def getSummary(bucket: CounterBucket): CounterSummary = bucket.summary

}

case class HistogramTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[HistogramBucket, StatisticSummary] with HistogramBucketSupport with StatisticSummarySupport {

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[HistogramBucket]): HistogramBucket = new HistogramBucket(bucketNumber, buckets)

  override def getSummary(bucket: HistogramBucket): StatisticSummary = bucket.summary
}
