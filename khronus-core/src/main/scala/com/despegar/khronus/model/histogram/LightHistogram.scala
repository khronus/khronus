package org.HdrHistogram

import java.nio.ByteBuffer

import com.despegar.khronus.model.histogram.{ Histogram ⇒ KhronusHistogram }
import com.esotericsoftware.kryo.io.Output

class LightHistogram(minAllowedValue: Long, maxAllowedValue: Long, precision: Int, private val frequencies: collection.mutable.Map[Int, Long] = collection.mutable.Map())
    extends AbstractHistogram(minAllowedValue, maxAllowedValue, precision) with KhronusHistogram {

  var totalCount: Long = 0

  override def +=(otherHistogram: KhronusHistogram): Unit = {
    otherHistogram match {
      case light: LightHistogram ⇒ add(light)
      case _                     ⇒ ???
    }
  }

  def add(lightHistogram: LightHistogram) = {
    lightHistogram.frequencies.foreach { entry ⇒
      val index = entry._1
      val count = entry._2
      val myCount = frequencies.getOrElseUpdate(index, 0)
      frequencies(index) = myCount + count
    }
    totalCount += lightHistogram.totalCount
    updatedMaxValue(lightHistogram.maxValue)
    updateMinNonZeroValue(lightHistogram.minNonZeroValue)
  }

  override def getCountAtIndex(index: Int): Long = {
    frequencies.get(index) match {
      case Some(count) ⇒ count
      case None        ⇒ 0
    }
  }

  override def incrementCountAtIndex(index: Int) = {
    val myCount = frequencies.getOrElseUpdate(index, 0)
    frequencies(index) = myCount + 1
  }

  override def incrementTotalCount(): Unit = totalCount += 1

  override def encodeIntoCompressedByteBuffer(targetBuffer: ByteBuffer): Int = ???

  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = ???

  override def addToCountAtIndex(index: Int, value: Long) = {

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

  override def setTotalCount(totalCount: Long): Unit = this.totalCount = totalCount

  override def getTotalCount(): Long = totalCount

  override def getCountAtNormalizedIndex(index: Int): Long = ???

  override def copyCorrectedForCoordinatedOmission(expectedIntervalBetweenValueSamples: Long): AbstractHistogram = ???

  override def _getEstimatedFootprintInBytes(): Int = ???

  override def fillBufferFromCountsArray(buffer: ByteBuffer, length: Int): Unit = ???

  override def fillCountsArrayFromBuffer(buffer: ByteBuffer, length: Int): Unit = ???

  override def copy(): AbstractHistogram = ???

  override def clearCounts(): Unit = ???

  override def setCountAtNormalizedIndex(index: Int, value: Long): Unit = ???

  override def addToCountAtNormalizedIndex(index: Int, value: Long): Unit = ???

  override def incrementCountAtNormalizedIndex(index: Int): Unit = ???

  override def addToTotalCount(value: Long): Unit = ???

}

