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

import com.despegar.khronus.util.Measurable
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.model.histogram.Histogram
import org.HdrHistogram.{ LightHistogram, SkinnyHdrHistogram }

import scala.util.{ Failure, Try }

class HistogramBucket(override val bucketNumber: BucketNumber, val histogram: Histogram) extends Bucket(bucketNumber) {

  import com.despegar.khronus.model.HistogramBucket._

  override def summary: HistogramSummary = Try {
    val p50 = histogram.getValueAtPercentile(50)
    val p80 = histogram.getValueAtPercentile(80)
    val p90 = histogram.getValueAtPercentile(90)
    val p95 = histogram.getValueAtPercentile(95)
    val p99 = histogram.getValueAtPercentile(99)
    val p999 = histogram.getValueAtPercentile(99.9)
    val min = histogram.getMinValue
    val max = histogram.getMaxValue
    val count = histogram.getTotalCount
    val mean = p50
    HistogramSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, mean.toLong)
  }.recoverWith[HistogramSummary] {
    case reason: Exception ⇒
      log.error(s"Failure creating summary of histogram: totalCount: ${histogram.getTotalCount}, " +
        s"$percentileDistribution", reason)
      Failure(reason)
  }.get

  private def percentileDistribution: String = {
    //    val baos = new ByteArrayOutputStream()
    //    val printStream = new PrintStream(baos)
    //    histogram.outputPercentileDistribution(printStream, 1000.0)
    //    printStream.flush()
    //    baos.toString
    histogram.toString
  }
}

object HistogramBucket extends Measurable with Logging {

  //114 years in milliseconds. wtf.  (3600000L is 1 hour in milliseconds)
  val histogramMaxValue = 3600000000000L

  //val histogramMaxValue = 36000000L

  val histogramPrecision = 3

  implicit def sumHistograms(buckets: Seq[HistogramBucket]): Histogram = {
    val histogram = new LightHistogram(1L, histogramMaxValue, histogramPrecision)
    val it = buckets.iterator
    while (it.hasNext) {
      val otherHisto = it.next()
      histogram += otherHisto.histogram
    }
    //buckets.foreach(bucket ⇒ histogram.add(bucket.histogram))
    histogram
  }

  def newHistogram = new SkinnyHdrHistogram(histogramMaxValue, histogramPrecision)
}

