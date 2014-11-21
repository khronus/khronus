package org.HdrHistogram

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import com.esotericsoftware.kryo.io.{ UnsafeInput, UnsafeOutput }

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
    val compressor = new Deflater(SkinnyHistogram.defaultCompressionLevel)
    compressor.setInput(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength)
    compressor.finish()
    val targetArray = targetBuffer.array()
    val compressedDataLength = compressor.deflate(targetArray, 12, targetArray.length - 12)
    compressor.end()
    targetBuffer.putInt(4, compressedDataLength)
    compressedDataLength + 12
  }

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val output = new UnsafeOutput(buffer.array())
    output.supportVarInts(true)
    output.writeVarInt(numberOfSignificantValueDigits, true)
    output.writeVarLong(lowestDiscernibleValue, true)
    output.writeLong(highestTrackableValue)
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

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): Histogram = {
    val cookie = buffer.getInt()
    if (cookie != encodingCompressedCookieBase) {
      buffer.rewind()
      return Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue)
    }
    val lengthOfCompressedContents = buffer.getInt()
    val lengthOfUnCompressedContents = buffer.getInt()

    val decompressor = new Inflater()
    decompressor.setInput(buffer.array(), 12, lengthOfCompressedContents);
    val decompressedBuffer = ByteBuffer.allocate(lengthOfUnCompressedContents);
    decompressor.inflate(decompressedBuffer.array());

    return decodeFromByteBuffer(decompressedBuffer)
  }

  def decodeFromByteBuffer(buffer: ByteBuffer): Histogram = {
    val input = new UnsafeInput(buffer.array(), 0, buffer.limit())
    input.setVarIntsEnabled(true)
    val significantValueDigits = input.readVarInt(true)
    val lowest = input.readVarLong(true)
    val highest = input.readLong()
    val totalCount = input.readVarLong(true)
    val idxArrayLength = input.readVarInt(true)

    val skinnyHistogram = new SkinnyHistogram(lowest, highest, significantValueDigits)
    skinnyHistogram.clearCounts()
    var lastIdx = 0
    var lastFreq = 0L
    (1 to idxArrayLength) foreach { _ ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq
      skinnyHistogram.addToCountAtIndex(idx, freq)
      lastIdx = idx
      lastFreq = freq
    }
    skinnyHistogram.setTotalCount(totalCount)

    skinnyHistogram
  }

}