package org.HdrHistogram

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import com.despegar.khronus.util.Pool
import com.esotericsoftware.kryo.io.{ Input, Output }

class SkinnyHistogram(lowestValue: Long, maxValue: Long, precision: Int) extends Histogram(lowestValue, maxValue, precision) {

  def this(maxValue: Long, precision: Int) {
    this(1L, maxValue, precision)
  }

  override def encodeIntoCompressedByteBuffer(targetBuffer: ByteBuffer): Int = {
    val intermediateUncompressedByteBuffer = ByteBuffer.allocate(this.getNeededByteBufferCapacity())
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

  //COPY PASTED FROM HDR SINCE IS PRIVATE THERE
  private def countsArrayIndex(value: Long): Int = {
    val bucketIndex: Int = getBucketIndex(value)
    val subBucketIndex: Int = getSubBucketIndex(value, bucketIndex)
    return countsArrayIndex(bucketIndex, subBucketIndex)
  }

  //COPY PASTED FROM HDR SINCE IS PRIVATE THERE
  private def countsArrayIndex(bucketIndex: Int, subBucketIndex: Int): Int = {
    assert((subBucketIndex < subBucketCount))
    assert((bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount)))
    val bucketBaseIndex: Int = (bucketIndex + 1) << subBucketHalfCountMagnitude
    val offsetInBucket: Int = subBucketIndex - subBucketHalfCount
    return bucketBaseIndex + offsetInBucket
  }

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val output = new Output(buffer.array())

    val maxValue: Long = getMaxValue
    val relevantLength = countsArrayIndex(maxValue) + 1

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
    output.flush()
    val total = output.total().toInt
    output.close()
    total
  }

  private def countsDiffs: Seq[(Int, Long)] = {
    var seq = Seq[(Int, Long)]()
    var lastValue: Long = 0
    var lastIdx: Int = 0
    for (i ← (0 to (counts.length - 1))) {
      val (idx, value) = (i, counts(i))
      if (value > 0) {
        seq = seq :+ ((idx - lastIdx), (value - lastValue))
        lastIdx = idx
        lastValue = value
      }
    }
    seq
  }

}

object SkinnyHistogram {
  private val encodingCompressedCookieBase: Int = 130
  private val defaultCompressionLevel = -1
  private val inflatersPool = Pool[Inflater]("inflatersPool", () ⇒ new Inflater(), 4, {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", () ⇒ new Deflater(defaultCompressionLevel), 4, {
    _.reset()
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

    val skinnyHistogram = new SkinnyHistogram(lowest, highest, significantValueDigits)
    skinnyHistogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio)
    skinnyHistogram.resetNormalizingIndexOffset(normalizingIndexOffset)
    var lastIdx = 0
    var lastFreq = 0L
    (1 to idxArrayLength) foreach { _ ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq
      skinnyHistogram.setCountAtNormalizedIndex(idx, freq)
      lastIdx = idx
      lastFreq = freq
    }
    skinnyHistogram.setTotalCount(totalCount)

    skinnyHistogram.establishInternalTackingValues()

    skinnyHistogram
  }

}