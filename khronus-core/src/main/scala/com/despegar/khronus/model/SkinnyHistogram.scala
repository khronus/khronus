package org.HdrHistogram

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import com.despegar.khronus.model.HistogramBucket
import com.despegar.khronus.util.{ Pool, Settings }
import com.esotericsoftware.kryo.io.{ Input, Output }
import net.jpountz.lz4.LZ4Factory

import scala.collection.mutable

class SkinnyHistogram(lowestValue: Long, maximumValue: Long, precision: Int) extends Histogram(lowestValue, maximumValue, precision) {

  import org.HdrHistogram.SkinnyHistogram._

  override def getCountAtIndex(index: Int): Long = {
    counts(index)
  }

  def this(maxValue: Long, precision: Int) {
    this(1L, maxValue, precision)
  }

  override def add(histogram: AbstractHistogram): Unit = {
    /**
     * if (!histogram.isInstanceOf[SkinnyHistogram] || histogram.lowestDiscernibleValue != lowestValue ||
     * histogram.highestTrackableValue != maximumValue || histogram.numberOfSignificantValueDigits != precision) {
     * super.add(histogram)
     * return
     * }
     */
    if (histogram.isInstanceOf[LightSkinnyHistogram]) {
      histogram.asInstanceOf[LightSkinnyHistogram].addHere(this)
      return
    }

    val otherSkinny = histogram

    var observedOtherTotalCount: Long = 0

    var idx: Int = 0
    val length = otherSkinny.countsArrayLength
    val totalCount = otherSkinny.getTotalCount
    while (observedOtherTotalCount < totalCount && idx < length) {

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

  //override it because we don't need synchronization
  override def updatedMaxValue(value: Long) = {
    if (value > maxValue) {
      maxValue = value
    }
  }

  //override it because we don't need synchronization
  override def updateMinNonZeroValue(value: Long) = {
    if (value < minNonZeroValue) {
      minNonZeroValue = value
    }
  }

  def getPercentiles(p1: Double, p2: Double, p3: Double, p4: Double, p5: Double, p6: Double): (Long, Long, Long, Long, Long, Long) = {
    val (p1value, p1index, p1accumm) = getPercentileValueIncrementalAt(p1)
    val (p2value, p2index, p2accumm) = getPercentileValueIncrementalAt(p2, p1index, p1accumm)
    val (p3value, p3index, p3accumm) = getPercentileValueIncrementalAt(p3, p2index, p2accumm)
    val (p4value, p4index, p4accumm) = getPercentileValueIncrementalAt(p4, p3index, p3accumm)
    val (p5value, p5index, p5accumm) = getPercentileValueIncrementalAt(p5, p4index, p4accumm)
    val (p6value, p6index, p6accumm) = getPercentileValueIncrementalAt(p6, p5index, p5accumm)

    (p1value, p2value, p3value, p4value, p5value, p6value)
  }

  def getPercentileValueIncrementalAt(percentile: Double, startIndex: Int = 0, accumulatedCount: Long = 0): (Long, Int, Long) = {
    val requestedPercentile: Double = Math.min(percentile, 100.0)
    var countAtPercentile: Long = (((requestedPercentile / 100.0) * getTotalCount) + 0.5).toLong
    countAtPercentile = Math.max(countAtPercentile, 1)
    var totalToCurrentIndex: Long = accumulatedCount
    var i: Int = startIndex
    while (i < countsArrayLength) {
      val countAtIndex = getCountAtIndex(i)
      val totalCurrent = totalToCurrentIndex
      totalToCurrentIndex += countAtIndex
      if (totalToCurrentIndex >= countAtPercentile) {
        val valueAtIndex: Long = valueFromIndex(i)
        return (highestEquivalentValue(valueAtIndex), i, totalCurrent)
      }
      i += 1
    }
    return (0, 0, 0)
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
  private val compressorsByName = Map[String, Compressor]("deflater" -> DeflaterCompressor, "lz4" -> LZ4Compressor)
  private val compressor: Compressor = compressorsByName.get(Settings.Histogram.Compressor).getOrElse(DeflaterCompressor)
  private val compressors = Map(DeflaterCompressor.version -> DeflaterCompressor, LZ4Compressor.version -> LZ4Compressor)

  val byteBuffersPool = Pool[ByteBuffer]("byteBuffersPool", 4, () ⇒ ByteBuffer.allocate(neededByteBufferCapacity), {
    _.clear()
  })

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): Histogram = {
    val version = buffer.getInt()

    compressors.get(version) match {
      case None ⇒ {
        buffer.rewind();
        Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue)
      }
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

    //val skinnyHistogram = new SkinnyHistogram(highest, significantValueDigits)
    //skinnyHistogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio)
    //skinnyHistogram.resetNormalizingIndexOffset(normalizingIndexOffset)
    var lastIdx = 0
    var lastFreq = 0L
    var minNonZeroIndex: Int = -1

    val indexes = Array.fill[Int](idxArrayLength)(0)
    val frequencies = Array.fill[Long](idxArrayLength)(0)

    (1 to idxArrayLength) foreach { i ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq

      indexes(i) = idx
      frequencies(i) = freq

      //skinnyHistogram.setCountAtNormalizedIndex(idx, freq)
      lastIdx = idx
      lastFreq = freq

      if (minNonZeroIndex != -1 && lastIdx != 0) {
        minNonZeroIndex = lastIdx
      }

    }
    /**
     * skinnyHistogram.resetMaxValue(0)
     * skinnyHistogram.resetMinNonZeroValue(Long.MaxValue)
     * if (lastIdx >= 0) {
     * skinnyHistogram.updatedMaxValue(skinnyHistogram.highestEquivalentValue(skinnyHistogram.valueFromIndex(lastIdx)))
     * }
     * if (minNonZeroIndex >= 0) {
     * skinnyHistogram.updateMinNonZeroValue(skinnyHistogram.valueFromIndex(minNonZeroIndex))
     * }
     *
     * skinnyHistogram.setTotalCount(totalCount)
     *
     * //skinnyHistogram
     */
    new LightSkinnyHistogram(indexes, frequencies, totalCount)
  }

}

class LightSkinnyHistogram(indexes: Array[Int], frequencies: Array[Long], totalCount: Long) extends Histogram(1, 2, 1) {

  def addHere(skinnyHistogram: SkinnyHistogram) = {
    var idx = 0
    while (idx < indexes.length) {
      val index = indexes(idx)
      val counts = frequencies(idx)
      skinnyHistogram.counts(index) += counts
      idx += 1
    }
  }

}

class AggregatedSkinnyHistogram() extends Histogram(1, 2, 1) {
  val indexes = mutable.SortedSet[Int]()
  val frequenciesMap = mutable.Map[Int, Long]()

  def add(index: Int, count: Long) = {
    indexes + index

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