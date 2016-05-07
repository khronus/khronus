package com.searchlight.khronus.model

import java.io.{ PrintStream, ByteArrayOutputStream }
import java.nio.ByteBuffer

import com.searchlight.khronus.util.LatencyTestUtil
import org.HdrHistogram.{ SkinnyHistogram, Histogram ⇒ HdrHistogram }

import scala.util.Random

object HdrHistogramTest extends App {

  precisionTest

  def printHistogram(histogram: HdrHistogram) = {
    val max = histogram.getHighestTrackableValue
    val expectedP50 = max * 0.5d
    println(s"maxTrackable: ${max}, precision: ${histogram.getNumberOfSignificantValueDigits}, " +
      s"p50: ${histogram.getValueAtPercentile(50)}, p90: ${histogram.getValueAtPercentile(90)}, p99: ${histogram.getValueAtPercentile(99)}, " +
      s"max: ${histogram.getMaxValue}")
  }

  def analyzePrecision(maxValue: Int, sampling: Double, outliersPercentage: Double, precisions: Seq[Int]) = {
    val histograms = precisions.map(precision ⇒ new HdrHistogram(maxValue, precision))
    (1 to (maxValue * sampling).toInt) foreach { n ⇒

      (1 to 10000) foreach (x ⇒ histograms.foreach(histogram ⇒ histogram.recordValue(n)))

    }
    val start = maxValue * 0.9d.toInt

    histograms.foreach { n ⇒
      n.recordValue(maxValue)
      printHistogram(n)
      serializeBucket(n)
    }
  }

  def precisionTest = {
    analyzePrecision(2, 1d, 1d, Seq(2, 3))
  }

  def doIt = {
    val oneHourInMicroseconds = 3600000000L
    val onePercentError = 2

    val histogram0 = new HdrHistogram(oneHourInMicroseconds, 2)
    val histogram1 = new HdrHistogram(oneHourInMicroseconds, 3)
    val histogram2 = new HdrHistogram(oneHourInMicroseconds, 3)
    val histogram3 = new HdrHistogram(3600000L, onePercentError)

    for (i ← 1 to 1000000) {
      val r = Random.nextInt(12500)
      histogram0.recordValue(r)
      histogram1.recordValue(r)
      histogram2.recordValue(r)
      histogram3.recordValue(r)
    }
    val a = serializeBucket(histogram0)
    val b = serializeBucket(histogram1)
    val c = serializeBucket(histogram2)
    val d = serializeBucket(histogram3)

    println(s"Reduction: ${100 - (c.toDouble / b.toDouble) * 100}%")

    val baos = new ByteArrayOutputStream()
    val printStream = new PrintStream(baos)
    histogram1.outputPercentileDistribution(printStream, 1000.0)
    printStream.flush()

    println(s"distribution: ${baos.toString()}")
  }

  def serializeBucket(histo: HdrHistogram): Int = {
    val encodedBytes = histo.encodeIntoByteBuffer(ByteBuffer.allocate(histo.getNeededByteBufferCapacity))
    val compressedBytes = histo.encodeIntoCompressedByteBuffer(ByteBuffer.allocate(histo.getNeededByteBufferCapacity))
    println(s"${histo.getHighestTrackableValue}:${histo.getNumberOfSignificantValueDigits}. Raw: $encodedBytes, deflated: $compressedBytes")

    compressedBytes
  }

  def sum() = {
    val testHistogram = new SkinnyHistogram(36000000L, 3)

    for (i ← 1 to 100) {
      val skinnyHistogram = new SkinnyHistogram(36000000L, 3)

      val histograms = for (i ← 1 to 100) yield {
        val h = new SkinnyHistogram(36000000L, 3)
        LatencyTestUtil.latencies foreach { latency ⇒
          h.recordValue(latency)
        }
        h
      }

      val start = System.currentTimeMillis()
      histograms.foreach(h ⇒ skinnyHistogram.add(h))
      testHistogram.recordValue(System.currentTimeMillis() - start)
    }

    println(s"Sum p95: ${testHistogram.getValueAtPercentile(95)}")
    println(s"Sum p999: ${testHistogram.getValueAtPercentile(999)}")
  }
}