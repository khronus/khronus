/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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
package com.searchlight.khronus.model

import java.io.{ ByteArrayOutputStream, PrintStream }

import com.searchlight.khronus.util.Measurable
import com.searchlight.khronus.util.log.Logging
import org.HdrHistogram.Histogram

import scala.util.{ Failure, Try }

class HistogramBucket(override val bucketNumber: BucketNumber, val histogram: Histogram) extends Bucket(bucketNumber) with Logging {

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
    val mean = histogram.getMean
    HistogramSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, mean.toLong)
  }.recoverWith[HistogramSummary] {
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

  private val DEFAULT_MIN_HISTOGRAM_SIZE = 2L
  private val DEFAULT_PRECISION = 3

  implicit def sumHistograms(buckets: Seq[HistogramBucket]): Histogram = measureTime("sumHistograms", "sumHistograms", false) {
    if (buckets.size == 1) buckets.head.histogram
    else {
      val histograms = collection.mutable.Buffer[Histogram]()
      buckets.foreach { bucket ⇒ histograms += bucket.histogram }
      val biggerHistogram = histograms.remove(biggerHistogramIndex(histograms))
      histograms.foreach(histogram ⇒ biggerHistogram.add(histogram))
      biggerHistogram
    }
  }

  implicit val HistoOrdering = new Ordering[Histogram] {
    override def compare(x: Histogram, y: Histogram): Int = {
      if (x.getMaxValue < y.getMaxValue)
        -1
      else if (x.getMaxValue > y.getMaxValue)
        1
      else
        0
    }
  }

  private def biggerHistogramIndex(histograms: Seq[Histogram]): Int = {
    histograms.zipWithIndex.maxBy(_._1)(HistoOrdering)._2
  }

  //1 hour in milliseconds
  def newHistogram = new Histogram(36000000L, 3)

  def newHistogram(value: Long) = new Histogram(closestPowerOfTwo(value), DEFAULT_PRECISION)

  private def closestPowerOfTwo(value: Long) = {
    val powerOfTwo = java.lang.Long.highestOneBit(value)
    if (powerOfTwo >= DEFAULT_MIN_HISTOGRAM_SIZE) {
      if (powerOfTwo >= value) powerOfTwo else powerOfTwo * 2
    } else {
      DEFAULT_MIN_HISTOGRAM_SIZE
    }
  }

}