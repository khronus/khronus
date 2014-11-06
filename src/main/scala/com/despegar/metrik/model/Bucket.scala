package com.despegar.metrik.model

import scala.concurrent.duration.Duration

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

}

object Timestamp {
  implicit def fromLong(ms: Long) = Timestamp(ms)
  implicit def fromInt(ms: Int) = Timestamp(ms.toLong)
}

case class BucketNumber(number: Long, duration: Duration) {
  //  def toTimestamp(aDuration: Duration): Timestamp = {
  //    Timestamp(aDuration.toMillis * number)
  //  }
  def startTimestamp(): Timestamp = {
    Timestamp(duration.toMillis * number)
  }
  def endTimestamp(): Timestamp = {
    Timestamp(duration.toMillis * (number + 1))
  }
  def <(otherBucketNumber: BucketNumber) = startTimestamp().ms < otherBucketNumber.startTimestamp().ms
  def >(otherBucketNumber: BucketNumber) = startTimestamp().ms > otherBucketNumber.startTimestamp().ms
}

object BucketNumber {
  implicit def fromIntTuple(tuple: (Int, Duration)) = BucketNumber(tuple._1, tuple._2)
  implicit def fromLongTuple(tuple: (Long, Duration)) = BucketNumber(tuple._1, tuple._2)
}
