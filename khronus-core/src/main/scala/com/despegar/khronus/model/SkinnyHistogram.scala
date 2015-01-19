package org.HdrHistogram

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import com.despegar.khronus.model.HistogramBucket
import com.despegar.khronus.util.Pool
import com.esotericsoftware.kryo.io.{ Input, Output }
import net.jpountz.lz4.LZ4Factory

class SkinnyHistogram(lowestValue: Long, maxValue: Long, precision: Int) extends Histogram(lowestValue, maxValue, precision) {

  override def getCountAtIndex(index: Int): Long = {
    counts(index)
  }

  def getPercentiles(percentiles: Seq[Double]): Seq[Long] = {
    val countAtPercentiles = percentiles.map { percentile ⇒
      val requestedPercentile = Math.min(percentile, 100.0)
      val countAtPercentile: Long = Math.max((((requestedPercentile / 100.0) * getTotalCount) + 0.5).toLong, 1)
      countAtPercentile
    }
    val percentilesLength = percentiles.length
    val output = Array.fill(percentiles.length)(0L)
    var idx = 0
    var totalToCurrentIndex: Long = 0
    var percentileIdx = 0
    while (idx < countsArrayLength && percentileIdx < percentilesLength) {
      totalToCurrentIndex += getCountAtIndex(idx)
      while (percentileIdx < percentilesLength && totalToCurrentIndex >= countAtPercentiles(percentileIdx)) {
        val valueAtIndex: Long = valueFromIndex(idx)
        output(percentileIdx) = highestEquivalentValue(valueAtIndex)
        percentileIdx += 1
      }
      idx += 1
    }
    output.toSeq
  }

  import org.HdrHistogram.SkinnyHistogram._

  def this(maxValue: Long, precision: Int) {
    this(1L, maxValue, precision)
  }

  override def add(histogram: AbstractHistogram): Unit = {
    val otherSkinny = histogram.asInstanceOf[SkinnyHistogram]

    var observedOtherTotalCount: Long = 0

    var idx: Int = 0
    while (idx < otherSkinny.countsArrayLength && observedOtherTotalCount < otherSkinny.getTotalCount) {

      val otherCount: Long = otherSkinny.getCountAtIndex(idx)
      if (otherCount > 0) {
        counts(idx) += otherCount
        observedOtherTotalCount += otherCount
      }
      idx += 1
    }

    setTotalCount(getTotalCount + observedOtherTotalCount)
    updatedMaxValue(Math.max(getMaxValue, otherSkinny.getMaxValue))
    updateMinNonZeroValue(Math.min(getMinNonZeroValue, otherSkinny.getMinNonZeroValue))
  }

  override def encodeIntoCompressedByteBuffer(targetBuffer: ByteBuffer): Int = {

    val intermediateUncompressedByteBuffer = byteBuffersPool.take()
    val uncompressedLength = this.encodeIntoByteBuffer(intermediateUncompressedByteBuffer)

    val compressedDataLength = lz4Factory.fastCompressor().compress(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength, targetBuffer.array(), 0)

    targetBuffer.putInt(encodingCompressedCookieBase)
    //targetBuffer.putInt(0)
    targetBuffer.putInt(uncompressedLength)
    //val compressor = deflatersPool.take()
    //compressor.setInput(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength)
    //compressor.finish()
    //val targetArray = targetBuffer.array()
    //val compressedDataLength = compressor.deflate(targetArray, 12, targetArray.length - 12)
    byteBuffersPool.release(intermediateUncompressedByteBuffer)
    //deflatersPool.release(compressor)

    targetBuffer.putInt(4, compressedDataLength)
    compressedDataLength + 12
  }

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val output = new Output(buffer.array())

    //val maxValue: Long = getMaxValue

    output.writeInt(normalizingIndexOffset)
    output.writeVarInt(numberOfSignificantValueDigits, true)
    output.writeVarLong(lowestDiscernibleValue, true)
    output.writeLong(highestTrackableValue)
    output.writeDouble(getIntegerToDoubleValueConversionRatio)
    output.writeVarLong(getTotalCount, true)

    val countsDiffsSeq = countsDiffs
    output.writeVarInt(countsDiffsSeq.length, true)
    countsDiffsSeq foreach { tuple ⇒
      val idx = tuple._1
      val freq = tuple._2
      output.writeVarInt(idx, true)
      output.writeVarLong(freq, false)
    }
    output.close()
    output.total().toInt
  }

  private def countsDiffs: Seq[(Int, Long)] = {
    var vectorBuilder = Vector.newBuilder[(Int, Long)]
    var lastValue: Long = 0
    var lastIdx: Int = 0
    var accum: Long = 0
    var idx = 0
    while (idx < counts.length && accum < totalCount) {
      val value = counts(idx)
      if (value > 0) {
        vectorBuilder += (((idx - lastIdx), (value - lastValue)))
        lastIdx = idx
        lastValue = value
        accum += value
      }
      idx = idx + 1
    }
    vectorBuilder.result()
  }

}

object SkinnyHistogram {
  private val lz4Factory = LZ4Factory.fastestInstance()
  private val encodingCompressedCookieBase: Int = 130
  private val defaultCompressionLevel = -1
  private val neededByteBufferCapacity = HistogramBucket.newHistogram.getNeededByteBufferCapacity
  private val inflatersPool = Pool[Inflater]("inflatersPool", 4, () ⇒ new Inflater(), {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", 4, () ⇒ new Deflater(defaultCompressionLevel), {
    _.reset()
  })

  val byteBuffersPool = Pool[ByteBuffer]("byteBuffersPool", 4, () ⇒ ByteBuffer.allocate(neededByteBufferCapacity), {
    _.clear()
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
    skinnyHistogram.resetNormalizingIndexOffset(normalizingIndexOffset)
    var lastIdx = 0
    var lastFreq = 0L
    var minNonZeroIndex: Int = -1
    (1 to idxArrayLength) foreach { _ ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq
      skinnyHistogram.setCountAtNormalizedIndex(idx, freq)
      lastIdx = idx
      lastFreq = freq

      if (minNonZeroIndex != -1 && lastIdx != 0) {
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