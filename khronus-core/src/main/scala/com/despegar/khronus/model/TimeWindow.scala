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
import com.despegar.khronus.util.{ ConcurrencySupport, Measurable, SameThreadExecutionContext }

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }

abstract class TimeWindow[T <: Bucket, U <: Summary] extends BucketStoreSupport[T] with SummaryStoreSupport[U] with MetaSupport with Logging with Measurable with BucketCacheSupport {

  import com.despegar.khronus.model.TimeWindow._

  private val durationStr = duration.toString

  def process(implicit metric: Metric, tick: Tick): Future[Unit] = {
    implicit val context = Context(metric, durationStr)

    measureFutureTime("processWindow", metric, duration) {

      log.debug(s"$context - Processing time window for ${Tick(tick.bucketNumber ~ duration)}")

      //get the last bucket processed for this window
      withLastProcessedBucket { lastProcessedBucket ⇒
        //retrieve the buckets from previous window
        withPreviousWindowBuckets(lastProcessedBucket) { previousWindowBuckets ⇒
          //group in buckets of my window duration
          //filter out buckets already processed. we don't want to override our precious buckets with late data
          val myBuckets = aggregateBuckets(grouped(previousWindowBuckets))
          //val myBuckets = aggregateBuckets(notProcessed(grouped(previousWindowBuckets), lastProcessedBucket))

          //calculate the summaries
          val mySummaries = myBuckets map (getSummary(_))

          //store temporal histogram buckets for next window if needed
          //store the summaries
          val stores = storeBucketsAndSummaries(myBuckets, mySummaries, lastProcessedBucket)

          stores.map { _ ⇒ bucketCache.markProcessedTick(metric, tick) }(TimeWindowChain.timeWindowExecutionContext)
        }
      }
    }
  }

  private def storeBucketsAndSummaries(buckets: Seq[T], summaries: Seq[U], lastProcessedBucket: BucketNumber)(implicit metric: Metric, tick: Tick, context: Context) = {
    storeTemporalBuckets(buckets, lastProcessedBucket) flatMap { _ ⇒
      summaryStore.store(metric, duration, summaries)
    }
  }

  private def withLastProcessedBucket[T](block: BucketNumber ⇒ Future[T])(implicit metric: Metric, tick: Tick, context: Context) = {
    lastProcessedBucket(metric) flatMap block
  }

  private def withPreviousWindowBuckets[B](lastProcessedBucket: BucketNumber)(block: Seq[(Timestamp, () ⇒ T)] ⇒ Future[B])(implicit metric: Metric, tick: Tick, context: Context) = {
    retrievePreviousBuckets(metric, tick, lastProcessedBucket) flatMap block
  }

  protected def getSummary(bucket: T): U

  private def storeTemporalBuckets(buckets: Seq[T], lastProcessedBucketNumber: BucketNumber)(implicit tick: Tick, metric: Metric, context: Context) = {
    if (shouldStoreTemporalHistograms) measureFutureTime("storeTemporalBuckets", metric, duration) {
      val storeFuture = bucketStore.store(metric, duration, buckets)
      storeFuture.onFailure { case reason: Throwable ⇒ log.error(s"$context - Fail to store temporal buckets", reason) }
      storeFuture.map { _ ⇒
        bucketCache.cacheBuckets(metric, lastProcessedBucketNumber ~ duration, tick.bucketNumber ~ duration, buckets)
      }(TimeWindowChain.timeWindowExecutionContext)
    }
    else {
      Future.successful[Unit](log.debug(s"$context - Last window. No need to store buckets"))
    }
  }

  protected def aggregateBuckets(buckets: Map[BucketNumber, Seq[T]]): Seq[T] = {
    buckets.collect { case (bucketNumber, buckets) ⇒ aggregate(bucketNumber, buckets) }.toSeq
  }

  protected def aggregate(bucketNumber: BucketNumber, buckets: Seq[T]): T

  def duration: Duration

  protected def previousWindowDuration: Duration

  protected def shouldStoreTemporalHistograms: Boolean

  private def retrievePreviousBuckets(metric: Metric, tick: Tick, lastProcessedBucketNumber: BucketNumber)(implicit context: Context) = {
    val start = System.currentTimeMillis()

    //we align both bucket numbers (lastProcessedBucketNumber and Tick) to the previousWindowDuration
    val fromBucketNumber = lastProcessedBucketNumber.endTimestamp().toBucketNumberOf(previousWindowDuration)
    //since the slices over bucketStore and bucketCache are exclusives at the end, we use the Tick's following bucket number as the end of the slices
    val toBucketNumber = tick.bucketNumber.following ~ duration ~ previousWindowDuration

    log.debug(s"$context - Slice [$fromBucketNumber, $toBucketNumber)")
    //TODO: refactor me
    bucketCache.take[T](metric, fromBucketNumber, toBucketNumber).map { buckets ⇒ Future.successful(buckets) }.getOrElse {
      val futureSlice = bucketStore.slice(metric, fromBucketNumber.startTimestamp(), toBucketNumber.startTimestamp(), previousWindowDuration)
      futureSlice.map { previousBuckets ⇒
        if (previousBuckets.isEmpty) {
          recordTime(formatLabel("emptySliceTime", metric, duration), System.currentTimeMillis() - start)
        }
        log.debug(s"$context - Found ${previousBuckets.size} buckets of $previousWindowDuration")
        previousBuckets

      }(TimeWindowChain.timeWindowExecutionContext)
    }
  }

  private def grouped(buckets: Seq[(Timestamp, () ⇒ T)])(implicit metric: Metric, context: Context): Map[BucketNumber, Seq[T]] = {
    val groupedBuckets = buckets.groupBy(tuple ⇒ tuple._1.toBucketNumberOf(duration)).mapValues(
      seq ⇒ seq.view.map(t ⇒ t._2()))
    if (!groupedBuckets.isEmpty) {
      log.debug(s"$context - Grouped ${groupedBuckets.size} buckets ${groupedBuckets.keys}")
    }
    groupedBuckets
  }

  private def notProcessed(groups: Map[BucketNumber, Seq[T]], lastBucket: BucketNumber)(implicit metric: Metric, context: Context): Map[BucketNumber, Seq[T]] = {
    val filteredBuckets = groups.filter(_._1.number > lastBucket.number)
    val filteredCount = groups.size - filteredBuckets.size
    if (filteredCount > 0) {
      log.warn(s"$context - Filtered out $filteredCount already processed buckets")
    }
    filteredBuckets
  }

  private def lastProcessedBucket(metric: Metric)(implicit context: Context): Future[BucketNumber] = {
    metaStore.getLastProcessedTimestamp(metric) map { lastTS ⇒
      val bucket = Timestamp(lastTS.ms - duration.toMillis).alignedTo(duration).toBucketNumberOf(duration)
      log.debug(s"$context - Last processed bucket: $bucket")
      bucket
    }
  }

}

object TimeWindow extends ConcurrencySupport {
  //implicit val executionContext: ExecutionContext = executionContext("time-window-worker")

  implicit val executionContext: ExecutionContext = SameThreadExecutionContext

}

case class CounterTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[CounterBucket, CounterSummary] with CounterBucketStoreSupport with CounterSummaryStoreSupport {

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[CounterBucket]): CounterBucket = new CounterBucket(bucketNumber, buckets)

  override def getSummary(bucket: CounterBucket): CounterSummary = bucket.summary

}

case class HistogramTimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
    extends TimeWindow[HistogramBucket, HistogramSummary] with HistogramBucketSupport with StatisticSummarySupport {

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[HistogramBucket]): HistogramBucket = new HistogramBucket(bucketNumber, buckets)

  override def getSummary(bucket: HistogramBucket): HistogramSummary = bucket.summary

}

case class Context(metric: Metric, durationStr: String) {
  override val toString = Logging.p(metric, durationStr)
}
