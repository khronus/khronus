package com.despegar.metrik.model

import org.HdrHistogram.Histogram

import scala.concurrent.duration.Duration

case class HistogramBucket(bucketNumer: Long, duration: Duration, histogram: Histogram) {
  def timestamp = bucketNumer * duration.toMillis
  def summary: StatisticSummary = {
    val p50 = histogram.getValueAtPercentile(50)
    val p80 = histogram.getValueAtPercentile(80)
    val p90 = histogram.getValueAtPercentile(90)
    val p95 = histogram.getValueAtPercentile(95)
    val p99 = histogram.getValueAtPercentile(99)
    val p999 = histogram.getValueAtPercentile(99.9)
    val min = histogram.getMinValue
    val max = histogram.getMaxValue
    val count = histogram.getTotalCount
    val avg = histogram.getMean()
    StatisticSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, avg)
  }
}

object HistogramBucket {
  implicit def sumHistograms(buckets: Seq[HistogramBucket]): Histogram = {
    val histogram = HistogramBucket.newHistogram
    buckets.foreach(bucket => histogram.add(bucket.histogram))
    histogram
  }
  def newHistogram = new Histogram(3600000000000L, 3)
}
