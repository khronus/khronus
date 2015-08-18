package org.HdrHistogram

import java.nio.ByteBuffer
import java.util
import java.util.zip.{ Deflater, Inflater }

import com.searchlight.khronus.model.HistogramBucket
import com.searchlight.khronus.util.Pool
import com.esotericsoftware.kryo.io.{ Input, Output }

import collection.JavaConverters._

class SkinnyHistogram(lowestValue: Long, maxValue: Long, precision: Int) extends Histogram(lowestValue, maxValue, precision) {

  private val cachedByteBuffer = new ThreadLocal[ByteBuffer]()

  def this(maxValue: Long, precision: Int) {
    this(1L, maxValue, precision)
    cachedByteBuffer.set(ByteBuffer.allocate(this.getNeededByteBufferCapacity()))
  }

  override def encodeIntoCompressedByteBuffer(targetBuffer: ByteBuffer): Int = {
    val intermediateUncompressedByteBuffer = cachedByteBuffer.get()
    intermediateUncompressedByteBuffer.clear()
    val uncompressedLength = this.encodeIntoByteBuffer(intermediateUncompressedByteBuffer)

    targetBuffer.putInt(SkinnyHistogram.encodingCompressedCookieBase)
    targetBuffer.putInt(0)
    targetBuffer.putInt(uncompressedLength)
    val compressor = SkinnyHistogram.deflatersPool.take()
    compressor.setInput(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength)
    compressor.finish()
    val targetArray = targetBuffer.array()
    val compressedDataLength = compressor.deflate(targetArray, 12, targetArray.length - 12)
    SkinnyHistogram.deflatersPool.release(compressor)

    targetBuffer.putInt(4, compressedDataLength)
    compressedDataLength + 12
  }

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val output = new Output(buffer.array())

    val maxValue: Long = getMaxValue
    val maxRelevantLength: Int = countsArrayIndex(maxValue) + 1
    val minRelevant = countsArrayIndex(minNonZeroValue)

    output.writeInt(normalizingIndexOffset)
    output.writeVarInt(numberOfSignificantValueDigits, true)
    output.writeVarLong(lowestDiscernibleValue, true)
    output.writeLong(highestTrackableValue)
    output.writeDouble(getIntegerToDoubleValueConversionRatio)
    output.writeVarLong(getTotalCount, true)

    val position_length = output.position()
    output.writeInt(1)

    var lastValue: Long = 0
    var lastIdx: Int = 0

    var i = minRelevant
    var count = 0
    while (i < maxRelevantLength) {
      val value = counts(i)
      if (value > 0) {
        output.writeVarInt(i - lastIdx, true)
        output.writeVarLong(value - lastValue, false)
        lastIdx = i
        lastValue = value
        count += 1
      }
      i += 1
    }
    val position_aftercounts = output.position()
    output.setPosition(position_length)
    output.writeInt(count)
    output.setPosition(position_aftercounts)

    output.flush()
    val total = output.total().toInt
    output.close()
    total
  }

}

object SkinnyHistogram {
  private val encodingCompressedCookieBase: Int = 130
  private val defaultCompressionLevel = -1
  private val inflatersPool = Pool[Inflater]("inflatersPool", 4, () ⇒ new Inflater(), {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", 4, () ⇒ new Deflater(defaultCompressionLevel), {
    _.end()
  })

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): Histogram = {
    val cookie = buffer.getInt()
    if (cookie != encodingCompressedCookieBase) {
      buffer.rewind()
      return Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue)
    }
    val lengthOfCompressedContents = buffer.getInt()
    val lengthOfUnCompressedContents = buffer.getInt()

    val decompressor = inflatersPool.take()
    decompressor.setInput(buffer.array(), 12, lengthOfCompressedContents);
    val decompressedBuffer = ByteBuffer.allocate(lengthOfUnCompressedContents);
    decompressor.inflate(decompressedBuffer.array());
    inflatersPool.release(decompressor)

    return decodeFromByteBuffer(decompressedBuffer)
  }

  def decodeFromByteBuffer(buffer: ByteBuffer): Histogram = {
    val input = new Input(buffer.array(), 0, buffer.limit())

    val normalizingIndexOffset = input.readInt()
    val significantValueDigits = input.readVarInt(true)
    val lowest = input.readVarLong(true)
    val highest = input.readLong()
    val integerToDoubleValueConversionRatio = input.readDouble()
    val totalCount = input.readVarLong(true)
    val idxArrayLength = input.readVarInt(true)

    val skinnyHistogram = HistogramBucket.newHistogram
    skinnyHistogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio)
    var lastIdx = 0
    var lastFreq = 0L
    var minNonZeroIndex: Int = -1
    (1 to idxArrayLength) foreach { _ ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq
      skinnyHistogram.setCountAtNormalizedIndex(idx, freq)
      lastIdx = idx
      lastFreq = freq

      if (minNonZeroIndex == -1 && lastIdx != 0) {
        minNonZeroIndex = lastIdx
      }

    }
    skinnyHistogram.resetMaxValue(0)
    skinnyHistogram.resetMinNonZeroValue(Long.MaxValue)
    if (lastIdx >= 0) {
      skinnyHistogram.updatedMaxValue(skinnyHistogram.highestEquivalentValue(skinnyHistogram.valueFromIndex(lastIdx)))
    }
    if (minNonZeroIndex >= 0) {
      skinnyHistogram.updateMinNonZeroValue(skinnyHistogram.valueFromIndex(minNonZeroIndex))
    }

    skinnyHistogram.setTotalCount(totalCount)

    skinnyHistogram
  }

}