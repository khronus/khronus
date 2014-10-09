package com.despegar.metrik.model

import com.despegar.metrik.store.{CassandraStatisticSummaryStore, StatisticSummaryStore, CassandraHistogramBucketStore, HistogramBucketStore}
import org.HdrHistogram.Histogram
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.HistogramBucket._
import scala.concurrent.ExecutionContext.Implicits.global
import com.despegar.metrik.store.HistogramBucketSupport
import com.despegar.metrik.store.StatisticSummarySupport
import scala.concurrent.duration._


case class TimeWindow(duration: Duration, previousWindowDuration: Duration, shouldStoreTemporalHistograms: Boolean = true) extends HistogramBucketSupport with StatisticSummarySupport {

  def process(metric: String) = {
    //retrieve the temporal histogram buckets from previous window
    val previousWindowBuckets = histogramBucketStore.sliceUntilNow(metric, previousWindowDuration)

    //group histograms in buckets of my window duration
    val groupedHistogramBuckets = previousWindowBuckets map (buckets => buckets.groupBy(_.timestamp / duration.toMillis))
    
    //filter out buckets already processed. we don't want to override our precious buckets with late data
    val filteredGroupedHistogramBuckets = groupedHistogramBuckets map ( _.filterNot(alreadyProcessed(metric)) )

    //sum histograms on each bucket
    val resultingBuckets = filteredGroupedHistogramBuckets map (buckets => buckets.collect{case (bucketNumber, histogramBuckets) => HistogramBucket(bucketNumber, duration, histogramBuckets)}.toSeq)

    //store temporal histogram buckets for next window if needed
    if (shouldStoreTemporalHistograms) {
      resultingBuckets map (buckets => histogramBucketStore.store(metric, duration, buckets))
    }

    //calculate the statistic summaries (percentiles, min, max, etc...)
    val statisticsSummaries = resultingBuckets.map(buckets => buckets map (_.summary))

    //store the statistic summaries
    statisticsSummaries map (summaries => statisticSummaryStore.store(metric, duration, summaries))
   
    //remove previous histogram buckets
    previousWindowBuckets map (windows => histogramBucketStore.remove(metric, previousWindowDuration, windows))
  }
  
  private def alreadyProcessed(metric: String): PartialFunction[(Long,Seq[HistogramBucket]), Boolean] = {
    case (bucketNumber, _) => false //how? a query over the statistics summaries to get the last one? -> yes
  }

}