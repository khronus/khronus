package com.despegar.metrik.model

import java.nio.ByteBuffer
import java.util.zip.Deflater

import org.HdrHistogram.{Histogram, SkinnyHistogram}
import org.xerial.snappy.Snappy

import scala.util.Random

object HistogramTest extends App {

  doIt

  def doIt = {
    val oneHourInMicroseconds = 3600000000L
    val onePercentError = 2

    val histogram0 = new Histogram(oneHourInMicroseconds, 2)
    val histogram1 = new Histogram(3600000L, 3)
    val histogram2 = new SkinnyHistogram(oneHourInMicroseconds, 3)
    val histogram3 = new SkinnyHistogram(3600000L, onePercentError)
    
    for (i <- (1 to 100000)) {
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
//    b
  }

  def serializeBucket(histo: Histogram): Int = {
    val buffer = ByteBuffer.allocate(histo.getEstimatedFootprintInBytes)
    val bytesEncoded = histo.encodeIntoCompressedByteBuffer(buffer) //TODO: Find a better way to do this serialization
    println(s"histo: $histo serialized into $bytesEncoded")
    buffer.limit(bytesEncoded)
    buffer.rewind()
    val byteArray = new Array[Byte](bytesEncoded)
    val byteArray2 = new Array[Byte](bytesEncoded)
    buffer.get(byteArray)
    val compressed = Snappy.compress(byteArray)
    bytesEncoded


    val compressor = new Deflater(9);
    compressor.setInput(byteArray, 0, bytesEncoded);
    compressor.finish();
    val compressedDataLength = compressor.deflate(byteArray2, 0, bytesEncoded);
    compressor.end();
    //compressed.length

    println(s"kryo: $bytesEncoded, snappy: ${compressed.length} and deflater: $compressedDataLength")

    bytesEncoded
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)
}