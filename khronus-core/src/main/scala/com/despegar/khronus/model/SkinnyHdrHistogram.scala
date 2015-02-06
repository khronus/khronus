package org.HdrHistogram

import java.nio.ByteBuffer

import com.despegar.khronus.model.HistogramBucket
import com.despegar.khronus.util._
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.despegar.khronus.model.histogram.{ Histogram ⇒ KhronusHistogram }

class SkinnyHdrHistogram(lowestValue: Long, maximumValue: Long, precision: Int) extends Histogram(lowestValue, maximumValue, precision)
    with KhronusHistogram {

  import org.HdrHistogram.SkinnyHdrHistogram._

  def this(maxValue: Long, precision: Int) {
    this(1L, maxValue, precision)
  }

  override def +=(otherHistogram: KhronusHistogram): Unit = {
    otherHistogram match {
      case hdr: AbstractHistogram ⇒ add(hdr)
      case _                      ⇒ ???
    }
  }

  override def getCountAtIndex(index: Int): Long = {
    counts(index)
  }

  override def add(histogram: AbstractHistogram): Unit = {
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

object SkinnyHdrHistogram {
  private val neededByteBufferCapacity = HistogramBucket.newHistogram.getNeededByteBufferCapacity

  val headerSize = 12
  private val compressorsByName = Map[String, Compressor]("deflater" -> DeflaterCompressor, "lz4" -> LZ4Compressor)
  val compressor: Compressor = compressorsByName.get(Settings.Histogram.Compressor).getOrElse(DeflaterCompressor)
  private val compressors = Map(DeflaterCompressor.version -> DeflaterCompressor, LZ4Compressor.version -> LZ4Compressor)

  val byteBuffersPool = Pool[ByteBuffer]("byteBuffersPool", 4, () ⇒ ByteBuffer.allocate(neededByteBufferCapacity), {
    _.clear()
  })

  def decodeFromCompressedByteBuffer(buffer: ByteBuffer, minBarForHighestTrackableValue: Long): KhronusHistogram = {
    val version = buffer.getInt()

    compressors.get(version) match {
      case None ⇒ {
        buffer.rewind();
        //Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue)
        ???
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

  def decodeFromByteBuffer(buffer: ByteBuffer): KhronusHistogram = {
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

    val freqMap: collection.mutable.Map[Int, Long] = collection.mutable.Map()
    (0 to (idxArrayLength - 1)) foreach { i ⇒
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarLong(false) + lastFreq

      indexes(i) = idx
      frequencies(i) = freq

      freqMap + (idx -> freq)

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

    val light = new LightHistogram(lowest, highest, significantValueDigits, freqMap)
    light.setTotalCount(totalCount)
    if (lastIdx >= 0) {
      light.updatedMaxValue(light.highestEquivalentValue(light.valueFromIndex(lastIdx)))
    }
    if (minNonZeroIndex >= 0) {
      light.updateMinNonZeroValue(light.valueFromIndex(minNonZeroIndex))
    }
    light
    //new LightSkinnyHistogram(indexes, frequencies, totalCount, minNonZeroIndex, lastIdx)
  }

}

class LightSkinnyHistogram(indexes: Array[Int], frequencies: Array[Long], totalCount: Long, minValueIndex: Int, maxValueIndex: Int) extends Histogram(1, 2, 1) {

  def addHere(skinnyHistogram: SkinnyHdrHistogram) = {
    var idx = 0
    while (idx < indexes.length) {
      val index = indexes(idx)
      val counts = frequencies(idx)
      skinnyHistogram.counts(index) += counts
      idx += 1
    }

    skinnyHistogram.totalCount += totalCount
    if (maxValueIndex >= 0) skinnyHistogram.updatedMaxValue(Math.max(skinnyHistogram.getMaxValue, skinnyHistogram.highestEquivalentValue(skinnyHistogram.valueFromIndex(maxValueIndex))))
    if (minValueIndex >= 0) skinnyHistogram.updateMinNonZeroValue(Math.min(skinnyHistogram.getMinNonZeroValue, skinnyHistogram.valueFromIndex(minValueIndex)))
  }

}

