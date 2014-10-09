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
    buckets.foreach(bucket ⇒ histogram.add(bucket.histogram))
    histogram
  }
  def newHistogram = new Histogram(3600000000000L, 3)
}
