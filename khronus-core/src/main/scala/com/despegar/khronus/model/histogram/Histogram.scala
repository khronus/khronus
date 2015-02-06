package com.despegar.khronus.model.histogram

import java.nio.ByteBuffer

trait Histogram {

  def recordValue(value: Long)

  def +=(otherHistogram: Histogram)

  def getMinValue: Long

  def getMaxValue: Long

  def getTotalCount(): Long

  def getValueAtPercentile(percentile: Double): Long

  def encodeIntoCompressedByteBuffer(byteBuffer: ByteBuffer): Int

}

