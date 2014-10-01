package com.despegar.metrik.model

import scala.concurrent.duration.Duration
import com.despegar.metrik.store.HistogramBucketStore
import org.HdrHistogram.Histogram
import com.despegar.metrik.store.StatisticSummaryStore

class Window(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true) {

  val zeroTime = 1970

  def process(metric: String) = {
    //retrieve the temporal histogram buckets from previous window
    val histograms: Seq[HistogramBucket] = HistogramBucketStore.sliceUntilNow(metric, previousWindowDuration)

    //group histograms in buckets of my window duration
    val groups: Map[Long, Seq[HistogramBucket]] = histograms.groupBy{ histogramBucket => (histogramBucket.timestamp - zeroTime) / duration.toMillis }

    //sum histograms on each bucket
    val resultingBuckets = groups.collect{ case (bucketNumber, histogramBuckets) => HistogramBucket(bucketNumber * duration.toMillis + zeroTime, sum(histogramBuckets)) }.toSeq
    
    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      HistogramBucketStore.store(metric, duration, resultingBuckets)
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