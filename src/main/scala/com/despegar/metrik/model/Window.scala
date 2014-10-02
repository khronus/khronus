package com.despegar.metrik.model

import com.despegar.metrik.store.{HistogramBucketStore, StatisticSummaryStore}
import org.HdrHistogram.Histogram

import scala.concurrent.duration.Duration

/**
 * Represents a time window interval
 *
 * @param duration
 * @param previousWindowDuration
 * @param shouldStoreTemporalHistograms
 */
class Window(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true)  extends  HistogramBucketSupport {

  val zeroTime = 1970


  def process(metric: String) = {
    //retrieve the temporal histogram buckets from previous window
    val histograms: Seq[HistogramBucket] = histogramBucketStore.sliceUntilNow(metric, previousWindowDuration)

    //group histograms in buckets of my window duration
    val groups: Map[Long, Seq[HistogramBucket]] = histograms.groupBy{ histogramBucket => histogramBucket.timestamp / duration.toMillis }

    //sum histograms on each bucket
    val resultingBuckets = groups.collect{ case (bucketNumber, histogramBuckets) => HistogramBucket(bucketNumber, duration, sum(histogramBuckets)) }.toSeq

    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      histogramBucketStore.store(metric, duration, resultingBuckets)
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = resultingBuckets.map( histogramBucket => statisticSummary(histogramBucket) )

    //store the statistic summaries
    StatisticSummaryStore.store(statisticsSummaries)

  }

  private def sum(histogramBuckets: Seq[HistogramBucket]): Histogram = {
    null
  }

  private def statisticSummary(histogramBucket: HistogramBucket): StatisticSummary = {
    null
  }

}