package com.searchlight.khronus.jmh

import java.nio.ByteBuffer

import org.HdrHistogram.{ SkinnyHistogram, Histogram }
import org.openjdk.jmh.annotations.{ Benchmark, Scope, State }

@State(Scope.Thread)
class HistogramBenchmark {

  val hdrHistogram = new Histogram(36000000L, 3)
  val skinnyHistogram = new SkinnyHistogram(36000000L, 3)

  HistogramData.latencies foreach { latency ⇒
    hdrHistogram.recordValue(latency)
    skinnyHistogram.recordValue(latency)
  }

  val buffer = ByteBuffer.allocate(hdrHistogram.getNeededByteBufferCapacity())

  @Benchmark
  def hdr_encodeIntoCompressedByteBuffer(): Int = {
    buffer.clear();
    hdrHistogram.encodeIntoCompressedByteBuffer(buffer)
  }

  @Benchmark
  def skinny_encodeIntoCompressedByteBuffer(): Int = {
    buffer.clear();
    skinnyHistogram.encodeIntoCompressedByteBuffer(buffer)
  }

  @Benchmark
  def hdr_encodeIntoByteBuffer(): Int = {
    buffer.clear();
    hdrHistogram.encodeIntoByteBuffer(buffer)
  }

  @Benchmark
  def skinny_encodeIntoByteBuffer(): Int = {
    buffer.clear();
    skinnyHistogram.encodeIntoByteBuffer(buffer)
  }

}