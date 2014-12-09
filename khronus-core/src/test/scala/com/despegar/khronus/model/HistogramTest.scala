package com.despegar.khronus.model

import java.io.{ PrintStream, ByteArrayOutputStream }
import java.nio.ByteBuffer

import org.HdrHistogram.{ SkinnyHistogram, Histogram }

import scala.util.Random

object HistogramTest extends App {

  doIt

  def doIt = {
    val oneHourInMicroseconds = 3600000000L
    val onePercentError = 2

    val histogram0 = new Histogram(oneHourInMicroseconds, 2)
    val histogram1 = new Histogram(oneHourInMicroseconds, 3)
    val histogram2 = new SkinnyHistogram(oneHourInMicroseconds, 3)
    val histogram3 = new SkinnyHistogram(3600000L, onePercentError)

    for (i ← (1 to 1000000)) {
      val r = Random.nextInt(250)
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

  def serializeBucket(histo: Histogram): Int = {
    val encodedBytes = histo.encodeIntoByteBuffer(ByteBuffer.allocate(histo.getEstimatedFootprintInBytes))
    val compressedBytes = histo.encodeIntoCompressedByteBuffer(ByteBuffer.allocate(histo.getEstimatedFootprintInBytes))
    println(s"$histo serialized into $compressedBytes")
    println(s"Raw: $encodedBytes, deflated: $compressedBytes")

    compressedBytes
  }

}