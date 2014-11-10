package org.HdrHistogram

import java.nio.ByteBuffer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{UnsafeInput, UnsafeOutput}

class SkinnyHistogram(maxValue: Long, precision: Int) extends Histogram(maxValue, precision) {

  val encodingCookieBase = 0x1c849308
  
  override def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    val (idxArray, freqArray) = createArrays

    val output = new UnsafeOutput(buffer.array())
    output.supportVarInts(true)
    output.writeVarInt(0x1c849308+ (wordSizeInBytes << 4), true)
    output.writeVarInt(numberOfSignificantValueDigits.toInt, true)
    output.writeVarLong(lowestTrackableValue, true)
    output.writeVarLong(highestTrackableValue, true)
    output.writeVarLong(getTotalCount, true)
    output.writeVarInt(idxArray.length, true)
    idxArray.zip(freqArray) foreach { tuple =>
      val idx = tuple._1
      val freq = tuple._2
      output.writeVarInt(idx, true)
      output.writeVarInt(freq.toInt, false)
    }
    output.flush()
    output.total().toInt
  }


  private def createArrays: (Array[Int], Array[Long]) = {
    var idxArray = Seq[Int]()
    var freqArray = Seq[Long]()
    var lastValue: Long = 0
    var lastIdx: Int = 0
    
    for ( i <- (0 to  (counts.length - 1)) ) {
        val (idx, value) = (i, counts(i))
        if (value > 0) {
        idxArray = (idxArray :+ (idx - lastIdx)).asInstanceOf[Seq[Int]]
        freqArray = (freqArray :+ (value - lastValue) ).asInstanceOf[Seq[Long]]
        lastIdx = idx
        lastValue = value
      }
    }
    (idxArray.toArray, freqArray.toArray)
  }

}

object SkinnyHistogram {
  def decodeFromByteBuffer(buffer: ByteBuffer): SkinnyHistogram = {
    val input = new UnsafeInput(buffer.array())

    val cookie = input.readVarInt(true)
    val significantValueDigits = input.readVarInt(true)
    val lowest = input.readVarLong(true)
    val highest = input.readVarLong(true)
    val totalCount = input.readVarLong(true)

    val idxArrayLength = input.readVarInt(true)

    val skinnyHistogram = new SkinnyHistogram(highest, significantValueDigits)

    var lastIdx = 0
    var lastFreq = 0
    (1 to idxArrayLength) foreach { a =>
      val idx = input.readVarInt(true) + lastIdx
      val freq = input.readVarInt(false) + lastFreq

      skinnyHistogram.addToCountAtIndex(idx, freq)

      lastIdx = idx
      lastFreq = freq
    }

    skinnyHistogram
  }
}