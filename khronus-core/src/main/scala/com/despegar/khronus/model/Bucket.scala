package com.despegar.khronus.model

import scala.concurrent.duration.Duration
import com.despegar.khronus.util.log.Logging

abstract case class Bucket(bucketNumber: BucketNumber) {
  def timestamp = bucketNumber.startTimestamp()

  def summary: Summary
}

case class Timestamp(ms: Long) {
  def toBucketNumber(duration: Duration): BucketNumber = toBucketNumber(duration, Math.floor _)

  private def toBucketNumber(duration: Duration, f: Double ⇒ Double) = {
    if (ms < 0) {
      BucketNumber(-1, duration)
    } else {
      BucketNumber(f(ms.toDouble / duration.toMillis.toDouble).toLong, duration)
    }
  }

  /**
   * It returns a new timestamp aligned to the end of the last bucket of the given duration.
   * It is basically a floor of this timestamp with the given duration.
   */
  def alignedTo(duration: Duration) = toBucketNumber(duration, Math.floor _).startTimestamp()

  def -(someMs: Long) = Timestamp(ms - someMs)
}

object Timestamp {
  implicit def fromLong(ms: Long) = Timestamp(ms)

  implicit def fromInt(ms: Int) = Timestamp(ms.toLong)
}

case class BucketNumber(number: Long, duration: Duration) {

  import BucketNumber._

  def startTimestamp(): Timestamp = {
    Timestamp(duration.toMillis * number)
  }

  def endTimestamp(): Timestamp = {
    Timestamp(duration.toMillis * (number + 1))
  }

  def ~(duration: Duration) = startTimestamp().toBucketNumber(duration)

  def <(otherBucketNumber: BucketNumber) = startTimestamp().ms < otherBucketNumber.startTimestamp().ms

  def <=(otherBucketNumber: BucketNumber) = startTimestamp().ms <= otherBucketNumber.startTimestamp().ms

  def >(otherBucketNumber: BucketNumber) = startTimestamp().ms > otherBucketNumber.startTimestamp().ms

  def -(aNumber: Int): BucketNumber = BucketNumber(number - aNumber, duration)

  def +(aNumber: Int): BucketNumber = BucketNumber(number + aNumber, duration)

  override def toString() = s"BucketNumber($number, $duration) from ${date(startTimestamp().ms)} to ${date(endTimestamp().ms)}"

}

object BucketNumber extends Logging {
  implicit def fromIntTuple(tuple: (Int, Duration)) = BucketNumber(tuple._1, tuple._2)

  implicit def fromLongTuple(tuple: (Long, Duration)) = BucketNumber(tuple._1, tuple._2)
}
