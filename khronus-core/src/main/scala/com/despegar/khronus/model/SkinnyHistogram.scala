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
    if (!histogram.isInstanceOf[SkinnyHistogram]) {
      super.add(histogram)
      return
    }

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
    val uncompressedByteBuffer = byteBuffersPool.take()
    val uncompressedLength = this.encodeIntoByteBuffer(uncompressedByteBuffer)

    val compressedLength = compressor.compress(uncompressedByteBuffer, 0, uncompressedLength, targetBuffer, headerSize)
    byteBuffersPool.release(uncompressedByteBuffer)

    targetBuffer.putInt(compressor.version)
    targetBuffer.putInt(compressedLength)
    targetBuffer.putInt(uncompressedLength)

    headerSize + compressedLength
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
  private val neededByteBufferCapacity = HistogramBucket.newHistogram.getNeededByteBufferCapacity

  private val headerSize = 12
  private val compressor: Compressor = DeflaterCompressor
  private val compressors = Map(DeflaterCompressor.version -> DeflaterCompressor, LZ4Compressor.version -> LZ4Compressor)

  val byteBuffersPool = Pool[ByteBuffer]("byteBuffersPool", 4, () ⇒ ByteBuffer.allocate(neededByteBufferCapacity), {
    _.clear()
  })

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): Histogram = {
    val version = buffer.getInt()

    compressors.get(version) match {
      case None ⇒ { buffer.rewind(); Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue) }
      case Some(selectedCompressor) ⇒ {
        val compressedLength = buffer.getInt()
        val uncompressedLength = buffer.getInt()

        val decompressedBuffer = ByteBuffer.allocate(uncompressedLength);
        selectedCompressor.decompress(buffer, headerSize, compressedLength, decompressedBuffer)

        decodeFromByteBuffer(decompressedBuffer)
      }
    }
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

    val skinnyHistogram = new SkinnyHistogram(highest, significantValueDigits)
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

trait Compressor {

  def version: Int
  def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int
  def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer)

}

object DeflaterCompressor extends Compressor {

  private val defaultCompressionLevel = -1
  private val encodingCompressedCookieBase: Int = 130

  private val inflatersPool = Pool[Inflater]("inflatersPool", 4, () ⇒ new Inflater(), {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", 4, () ⇒ new Deflater(defaultCompressionLevel), {
    _.reset()
  })

  override def version = encodingCompressedCookieBase

  override def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int = {
    val deflater = deflatersPool.take()
    deflater.setInput(inputBuffer.array(), inputOffset, inputLength)
    deflater.finish()
    val targetArray = targetBuffer.array()
    val compressedLength = deflater.deflate(targetArray, targetOffset, targetArray.length - targetOffset)
    deflatersPool.release(deflater)
    compressedLength
  }

  override def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer): Unit = {
    val inflater = inflatersPool.take()

    inflater.setInput(inputBuffer.array(), inputOffset, inputLength)
    inflater.inflate(targetBuffer.array())

    inflatersPool.release(inflater)
  }
}

object LZ4Compressor extends Compressor {
  private val lz4Factory = LZ4Factory.fastestInstance()

  val version: Int = 230

  override def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int = {
    lz4Factory.fastCompressor().compress(inputBuffer.array(), inputOffset, inputLength, targetBuffer.array(), targetOffset)
  }

  override def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer): Unit = {
    lz4Factory.fastDecompressor().decompress(inputBuffer.array(), inputOffset, targetBuffer.array(), 0, targetBuffer.array().length)
  }
}