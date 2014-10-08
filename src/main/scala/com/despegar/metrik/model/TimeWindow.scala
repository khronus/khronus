package com.despegar.metrik.model

import com.despegar.metrik.store.{CassandraStatisticSummaryStore, StatisticSummaryStore, CassandraHistogramBucketStore, HistogramBucketStore}
import org.HdrHistogram.Histogram
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.HistogramBucket._
import com.despegar.metrik.store.HistogramBucketSupport
import com.despegar.metrik.store.StatisticSummarySupport

class TimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true) extends HistogramBucketSupport with StatisticSummarySupport {

  def process(metric: String) = {
    //retrieve the temporal histogram buckets from previous window
    val histogramBuckets = histogramBucketStore.sliceUntilNow(metric, previousWindowDuration)

    //group histograms in buckets of my window duration
    val groupedHistogramBuckets = histogramBuckets.groupBy(_.timestamp / duration.toMillis)

    //sum histograms on each bucket
    val resultingBuckets = groupedHistogramBuckets.collect{case (bucketNumber, histogramBuckets) => HistogramBucket(bucketNumber, duration, histogramBuckets)}.toSeq

    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      histogramBucketStore.store(metric, duration, resultingBuckets)
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = resultingBuckets.map(_.summary)

    //store the statistic summaries
    statisticSummaryStore.store(statisticsSummaries)
  }

}