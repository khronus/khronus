package org.HdrHistogram

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import com.esotericsoftware.kryo.io._
import com.searchlight.khronus.util.Pool

/**
 * SkinnyHistogram is an extended version of the HdrHistogram with optimizations to improve serialization times and compression.
 * It discards zero frequency entries, encodes variable integers and longs, uses delta encoding for indexes and frequencies and
 * reuses deflaters/inflaters instances.
 */
class SkinnyHistogram(lowestValue: Long, maxValue: Long, precision: Int) extends Histogram(lowestValue, maxValue, precision) {

  private var byteBuffer: ByteBuffer = _

  override def encodeIntoCompressedByteBuffer(targetBuffer: ByteBuffer): Int = {
    val intermediateUncompressedByteBuffer = buffer()
    val uncompressedLength = encodeIntoByteBuffer(intermediateUncompressedByteBuffer)

    val headerBytes = SkinnyHistogram.headerSize
    //header: 12 bytes
    targetBuffer.putInt(SkinnyHistogram.encodingCompressedCookieBase) //4
    targetBuffer.putInt(0) //4
    targetBuffer.putInt(uncompressedLength) // 4

    val compressor = SkinnyHistogram.deflatersPool.take()
    compressor.setInput(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength)
    compressor.finish()
    val targetArray = targetBuffer.array()
    val compressedDataLength = compressor.deflate(targetArray, headerBytes, targetArray.length - headerBytes)
    SkinnyHistogram.deflatersPool.release(compressor)

    targetBuffer.putInt(4, compressedDataLength)
    compressedDataLength + headerBytes
  }

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val output = new Output(buffer.array())

    val maxRelevantLength = countsArrayIndex(getMaxValue) + 1
    val minRelevant = countsArrayIndex(minNonZeroValue)

    output.writeInt(normalizingIndexOffset)
    output.writeVarInt(numberOfSignificantValueDigits, true)
    output.writeVarLong(lowestDiscernibleValue, true)
    output.writeLong(highestTrackableValue)
    output.writeDouble(getIntegerToDoubleValueConversionRatio)
    output.writeVarLong(getTotalCount, true)

    val frequenciesArrayLengthPosition = output.position()
    output.writeInt(1) //reserve 4 bytes to be filled with frequenciesArrayLength after the loop

    var lastFrequency: Long = 0
    var lastIdx: Int = 0

    var index = minRelevant
    var frequenciesArrayLength = 0
    while (index < maxRelevantLength) {
      val frequency = counts(index)
      if (frequency > 0) {
        output.writeVarInt(index - lastIdx, true)
        output.writeVarLong(frequency - lastFrequency, false)
        lastIdx = index
        lastFrequency = frequency
        frequenciesArrayLength += 1
      }
      index += 1
    }
    val finalPosition = output.position()

    output.setPosition(frequenciesArrayLengthPosition)
    output.writeInt(frequenciesArrayLength)

    output.setPosition(finalPosition)

    output.total().toInt
  }

  private def buffer() = {
    val relevantLength = countsArrayIndex(getMaxValue) + 1
    val neededCapacity = getNeededByteBufferCapacity(relevantLength)
    if (byteBuffer == null || byteBuffer.capacity() < neededCapacity) {
      byteBuffer = ByteBuffer.allocate(neededCapacity)
    }
    byteBuffer.clear()
    byteBuffer
  }

}

object SkinnyHistogram {
  private val headerSize = 12
  private val encodingCompressedCookieBase: Int = 130
  private val defaultCompressionLevel = -1
  private val inflatersPool = Pool[Inflater]("inflatersPool", 4, () ⇒ new Inflater(), {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", 4, () ⇒ new Deflater(defaultCompressionLevel), {
    _.reset()
  })

  def apply(maxValue: Long, precision: Int): SkinnyHistogram = apply(1L, maxValue, precision)

  def apply(lowest: Long, maxValue: Long, precision: Int): SkinnyHistogram = new SkinnyHistogram(lowest, maxValue, precision)

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): Histogram = {
    val cookie = buffer.getInt
    if (cookie != encodingCompressedCookieBase) {
      buffer.rewind()
      return Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue)
    }
    val compressedBytes = buffer.getInt
    val uncompressedBytes = buffer.getInt

    val decompressor = inflatersPool.take()

    decompressor.setInput(buffer.array(), headerSize, compressedBytes)
    val decompressedBuffer = ByteBuffer.allocate(uncompressedBytes)
    decompressor.inflate(decompressedBuffer.array())

    inflatersPool.release(decompressor)

    decodeFromByteBuffer(decompressedBuffer)
  }

  def decodeFromByteBuffer(buffer: ByteBuffer): Histogram = {
    val input = new Input(buffer.array(), 0, buffer.limit())

    val normalizingIndexOffset = input.readInt()
    val significantValueDigits = input.readVarInt(true)
    val lowest = input.readVarLong(true)
    val highest = input.readLong()
    val integerToDoubleValueConversionRatio = input.readDouble()
    val totalCount = input.readVarLong(true)
    val frequenciesArrayLength = input.readInt()

    val skinnyHistogram = SkinnyHistogram(lowest, highest, significantValueDigits)
    skinnyHistogram.setNormalizingIndexOffset(normalizingIndexOffset)
    skinnyHistogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio)
    var lastIdx = 0
    var lastFrequency = 0L
    var minNonZeroIndex: Int = -1

    var i = 1
    while (i <= frequenciesArrayLength) {
      val idx = input.readVarInt(true) + lastIdx
      val frequency = input.readVarLong(false) + lastFrequency
      skinnyHistogram.setCountAtNormalizedIndex(idx, frequency)
      lastIdx = idx
      lastFrequency = frequency

      if (minNonZeroIndex == -1 && lastIdx != 0) {
        minNonZeroIndex = lastIdx
      }
      i += 1
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