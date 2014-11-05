package org.HdrHistogram

import java.nio.ByteBuffer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.UnsafeOutput

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
    idxArray foreach { i =>  output.writeVarInt(i, true)}
    freqArray foreach { i =>  output.writeVarLong(i, true)}
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
        freqArray = (freqArray :+ (value) ).asInstanceOf[Seq[Long]]
        lastIdx = idx
        lastValue = value
      }
    }
    (idxArray.toArray, freqArray.toArray)
  }

}