package com.despegar.metrik.model

import org.HdrHistogram.Histogram
import org.mockito.{ Matchers, ArgumentMatcher, ArgumentCaptor, Mockito }
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{ FunSuite }
import org.scalatest.mock.MockitoSugar
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import scala.util.Random
import java.nio.ByteBuffer
import org.HdrHistogram.SkinnyHistogram

object HistogramTest extends App {

  doIt

  def doIt = {
    val histogram1 = new Histogram(3600000000000L, 3)
    val histogram2 = new SkinnyHistogram(3600000000000L, 3)
    
    for (i <- (1 to 1000000)) {
      val r = Random.nextInt(100)
      histogram1.recordValue(r)
      histogram2.recordValue(r)
    }
    val b = serializeBucket(histogram1)
    val c = serializeBucket(histogram2)
    
    println(s"Reduction: ${100 - (c.toDouble / b.toDouble) * 100}%")
//    b
  }

  def serializeBucket(histo: Histogram): Int = {
    val buffer = ByteBuffer.allocate(histo.getEstimatedFootprintInBytes)
    val bytesEncoded = histo.encodeIntoCompressedByteBuffer(buffer) //TODO: Find a better way to do this serialization
    println(s"histo: $histo serialized into $bytesEncoded")
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
    bytesEncoded
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)
}