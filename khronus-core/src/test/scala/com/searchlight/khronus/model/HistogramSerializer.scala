package com.searchlight.khronus.model

import java.nio.ByteBuffer

import org.HdrHistogram.Histogram

trait HistogramSerializer {
  def serialize(histogram: Histogram): ByteBuffer
  def deserialize(byteBuffer: ByteBuffer): Histogram
}
