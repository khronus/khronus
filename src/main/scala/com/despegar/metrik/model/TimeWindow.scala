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

import com.despegar.metrik.store._
import org.HdrHistogram.Histogram
import com.despegar.metrik.model.HistogramBucket._
import com.despegar.metrik.util.Logging
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class TimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)
  extends HistogramBucketSupport with StatisticSummarySupport with MetaSupport with Logging {

  def process(metric: String): Future[Any] = {
    log.debug(s"Processing window of $duration for metric $metric...")
    //retrieve the temporal histogram buckets from previous window
    val previousWindowBuckets = histogramBucketStore.sliceUntilNow(metric, previousWindowDuration)

    //group histograms in buckets of my window duration
    def groupedHistogramBuckets = previousWindowBuckets map (buckets ⇒ buckets.groupBy(_.timestamp / duration.toMillis))

    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val filteredGroupedHistogramBuckets = for {
      lastBucketNumber ← getLastProcessedBucketNumber(metric)
      filteredHistograms ← groupedHistogramBuckets map (histogramsBuckets ⇒ histogramsBuckets.filterNot(_._1 <= lastBucketNumber))
    } yield {
      filteredHistograms
    }

    //sum histograms on each bucket
    val resultingBuckets = filteredGroupedHistogramBuckets map (buckets ⇒ buckets.collect { case (bucketNumber, histogramBuckets) ⇒ HistogramBucket(bucketNumber, duration, histogramBuckets) }.toSeq)

    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      resultingBuckets map (buckets ⇒ ifNotEmpty(buckets)(histogramBucketStore.store(metric, duration, buckets)))
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = resultingBuckets.map(buckets ⇒ buckets map (_.summary))

    //store the statistic summaries
    statisticsSummaries map (summaries ⇒ ifNotEmpty(summaries)(statisticSummaryStore.store(metric, duration, summaries))) andThen {
      case _ ⇒
        //remove previous histogram buckets
        previousWindowBuckets map (windows ⇒ ifNotEmpty(windows)(histogramBucketStore.remove(metric, previousWindowDuration, windows)))
    }
  }

  /**
   * Call the function f only if the collection is not empty
   */
  def ifNotEmpty(col: Seq[AnyRef])(f: ⇒ Future[Unit]) = {
    if (col.size > 0) {
      f
    }
  }

  /**
   * Returns the last bucket number found in statistics summaries
   * @param metric
   * @return a Long representing the bucket number. If nothing if found -1 is returned
   */
  private def getLastProcessedBucketNumber(metric: String): Future[Long] = {
    metaStore.getLastProcessedTimestamp(metric) map { timestamp => timestamp / duration.toMillis}
  }

}