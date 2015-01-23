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

import java.io.{ ByteArrayOutputStream, PrintStream }

import com.despegar.khronus.util.Measurable
import com.despegar.khronus.util.log.Logging
import org.HdrHistogram.{ Histogram, SkinnyHistogram }

import scala.util.{ Failure, Try }

class HistogramBucket(override val bucketNumber: BucketNumber, val histogram: Histogram) extends Bucket(bucketNumber) {

  import com.despegar.khronus.model.HistogramBucket._

  override def summary: StatisticSummary = Try {
    if (histogram.isInstanceOf[SkinnyHistogram]) {
      val (p50, p80, p90, p95, p99, p999) = histogram.asInstanceOf[SkinnyHistogram].getPercentiles(50, 80, 90, 95, 99, 99.9)
      val min = histogram.getMinValue
      val max = histogram.getMaxValue
      val count = histogram.getTotalCount
      val mean = p50
      StatisticSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, mean.toLong)
    } else {
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
      StatisticSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, mean.toLong)
    }

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

object HistogramBucket extends Measurable with Logging {

  //114 years in milliseconds. wtf.  (3600000L is 1 hour in milliseconds)
  val histogramMaxValue = 3600000000000L

  //val histogramMaxValue = 36000000L

  val histogramPrecision = 3

  implicit def sumHistograms(buckets: Seq[HistogramBucket]): Histogram = {
    val histogram = newHistogram
    if (buckets.length == 2) {
      histogram.add(buckets(0).histogram)
      histogram.add(buckets(1).histogram)
    } else {
      buckets.foreach(bucket ⇒ histogram.add(bucket.histogram))
    }
    histogram
  }

  def newHistogram = new SkinnyHistogram(histogramMaxValue, histogramPrecision)
}

