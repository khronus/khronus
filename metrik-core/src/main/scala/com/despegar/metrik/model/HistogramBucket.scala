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

import java.io.{ PrintStream, ByteArrayOutputStream, StringWriter }

import com.despegar.metrik.util.{ Logging, Measurable }
import org.HdrHistogram.{ Histogram, SkinnyHistogram }

import scala.util.{ Failure, Try }

class HistogramBucket(override val bucketNumber: BucketNumber, val histogram: Histogram) extends Bucket(bucketNumber) with Logging {

  override def summary: StatisticSummary = Try {
    val p50 = histogram.getValueAtPercentile(50)
    val p80 = histogram.getValueAtPercentile(80)
    val p90 = histogram.getValueAtPercentile(90)
    val p95 = histogram.getValueAtPercentile(95)
    val p99 = histogram.getValueAtPercentile(99)
    val p999 = histogram.getValueAtPercentile(99.9)
    val min = histogram.getMinValue
    val max = histogram.getMaxValue
    val count = histogram.getTotalCount
    val avg = histogram.getMean

    StatisticSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, avg.toLong)
  }.recoverWith[StatisticSummary] {
    case e: Exception ⇒

      val baos = new ByteArrayOutputStream()
      val printStream = new PrintStream(baos)
      histogram.outputPercentileDistribution(printStream, 1000.0)
      printStream.flush()
      log.error(s"Failure creating summary of histogram: totalCount: ${histogram.getTotalCount}, percentileDistribution: ${baos.toString}", e)
      Failure(e)
  }.get
}

object HistogramBucket extends Measurable {
  implicit def sumHistograms(buckets: Seq[HistogramBucket]): Histogram = measureTime("sumHistograms") {
    val histogram = HistogramBucket.newHistogram
    buckets.foreach(bucket ⇒ histogram.add(bucket.histogram))
    histogram
  }

  def newHistogram = new Histogram(3600000000000L, 3)
}

