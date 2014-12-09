package com.despegar.khronus.model

import scala.concurrent.duration._

trait TimeWindowTest[T <: Bucket] {

  val neverProcessedTimestamp = Timestamp(-1)
  val windowDuration: FiniteDuration = 30 seconds
  val previousWindowDuration: FiniteDuration = 1 millis

  protected def lazyBuckets(buckets: Seq[T]): Seq[() ⇒ T] = {
    buckets.map(bucket ⇒ () ⇒ bucket)
  }

  protected def uniqueTimestamps(buckets: Seq[T]): Seq[Timestamp] = {
    buckets.map(bucket ⇒ bucket.timestamp)
  }
}
