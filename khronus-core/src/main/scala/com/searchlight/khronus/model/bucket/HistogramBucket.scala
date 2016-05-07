package com.searchlight.khronus.model.bucket

import java.io.{ PrintStream, ByteArrayOutputStream }

import com.searchlight.khronus.model.summary.HistogramSummary
import com.searchlight.khronus.model.{ Bucket, BucketNumber }
import com.searchlight.khronus.util.Measurable
import com.searchlight.khronus.util.log.Logging

import scala.util.{ Failure, Try }
import org.HdrHistogram.{ Histogram ⇒ HdrHistogram }

case class HistogramBucket(bucketNumber: BucketNumber, histogram: HdrHistogram) extends Bucket with Logging {

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

  implicit def aggregate(bucketNumber: BucketNumber, buckets: Seq[HistogramBucket]): HistogramBucket = measureTime("sumHistograms", "sumHistograms", false) {
    val histo = if (buckets.size == 1) buckets.head.histogram
    else {
      val histograms = collection.mutable.Buffer[HdrHistogram]()
      buckets.foreach { bucket ⇒ histograms += bucket.histogram }
      val biggerHistogram = biggerHistogramOf(histograms)
      histograms.filterNot(_.equals(biggerHistogram)).foreach(histogram ⇒ biggerHistogram.add(histogram))
      biggerHistogram
    }
    HistogramBucket(bucketNumber, histo)
  }

  private def biggerHistogramOf(histograms: Seq[HdrHistogram]): HdrHistogram = {
    var biggerHistogram = histograms.head
    histograms.tail.foreach { histogram ⇒
      if (histogram.getMaxValue > biggerHistogram.getMaxValue) {
        biggerHistogram = histogram
      }
    }
    biggerHistogram
  }

  //1 hour in milliseconds
  def newHistogram = new HdrHistogram(36000000L, 3)

  def newHistogram(value: Long) = new HdrHistogram(closestPowerOfTwo(value), DEFAULT_PRECISION)

  private def closestPowerOfTwo(value: Long) = {
    val powerOfTwo = java.lang.Long.highestOneBit(value)
    if (powerOfTwo >= DEFAULT_MIN_HISTOGRAM_SIZE) {
      if (powerOfTwo >= value) powerOfTwo else powerOfTwo * 2
    } else {
      DEFAULT_MIN_HISTOGRAM_SIZE
    }
  }

}
