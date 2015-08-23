package com.searchlight.khronus.jmh

import java.nio.ByteBuffer

import org.HdrHistogram.{ SkinnyHistogram, Histogram }
import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
class HistogramBenchmark {

  //default: 1 hour in micros
  @Param(Array("3600000000"))
  var maxValue: Long = _

  @Param(Array("3"))
  var precision: Int = _

  @Param(Array("case1", "case2", "case3", "sparsed1", "sparsed2", "quadratic", "cubic", "case1PlusSparsed2"))
  var latencies: String = _

  var hdrHistogram: Histogram = _
  var skinnyHistogram: Histogram = _
  var buffer: ByteBuffer = _

  @Setup
  def prepare(): Unit = {
    hdrHistogram = new Histogram(maxValue, precision)
    skinnyHistogram = SkinnyHistogram(maxValue, precision)
    HistogramData.data(latencies) foreach { latency ⇒
      hdrHistogram.recordValue(latency)
      skinnyHistogram.recordValue(latency)
    }
    buffer = ByteBuffer.allocate(hdrHistogram.getNeededByteBufferCapacity)
    val skinnyBytes = skinny_encodeIntoCompressedByteBuffer()
    val hdrBytes = hdr_encodeIntoCompressedByteBuffer()

    println(s"compressed bytes: skinny=$skinnyBytes, hdr=$hdrBytes. Reduction: ${100 - (skinnyBytes.toDouble / hdrBytes.toDouble) * 100}%")
  }

  @Benchmark
  def hdr_encodeIntoCompressedByteBuffer(): Int = {
    buffer.clear()
    hdrHistogram.encodeIntoCompressedByteBuffer(buffer)
  }

  @Benchmark
  def skinny_encodeIntoCompressedByteBuffer(): Int = {
    buffer.clear()
    skinnyHistogram.encodeIntoCompressedByteBuffer(buffer)
  }

  @Benchmark
  def hdr_encodeIntoByteBuffer(): Int = {
    buffer.clear()
    hdrHistogram.encodeIntoByteBuffer(buffer)
  }

  @Benchmark
  def skinny_encodeIntoByteBuffer(): Int = {
    buffer.clear()
    skinnyHistogram.encodeIntoByteBuffer(buffer)
  }

  @Benchmark
  def skinny_roundtrip_uncompressed(): Histogram = {
    buffer.clear()
    skinnyHistogram.encodeIntoByteBuffer(buffer)
    buffer.rewind()
    SkinnyHistogram.decodeFromByteBuffer(buffer)
  }

  @Benchmark
  def skinny_roundtrip_compressed(): Histogram = {
    buffer.clear()
    skinnyHistogram.encodeIntoCompressedByteBuffer(buffer)
    buffer.rewind()
    SkinnyHistogram.decodeFromCompressedByteBuffer(buffer, maxValue)
  }

  @Benchmark
  def hdr_roundtrip_uncompressed(): Histogram = {
    buffer.clear()
    hdrHistogram.encodeIntoByteBuffer(buffer)
    buffer.rewind()
    Histogram.decodeFromByteBuffer(buffer, maxValue)
  }

  @Benchmark
  def hdr_roundtrip_compressed(): Histogram = {
    buffer.clear()
    hdrHistogram.encodeIntoCompressedByteBuffer(buffer)
    buffer.rewind()
    Histogram.decodeFromCompressedByteBuffer(buffer, maxValue)
  }
}