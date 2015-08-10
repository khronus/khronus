package com.searchlight.khronus.model

import scala.concurrent.duration._

trait TimeWindowTest[T <: Bucket] {

  val neverProcessedTimestamp = Timestamp(-1)
  val windowDuration: FiniteDuration = 30 seconds
  val previousWindowDuration: FiniteDuration = 1 millis

  protected def lazyBuckets(buckets: Seq[T]): Seq[LazyBucket[T]] = {
    buckets.map(bucket ⇒ new LazyBucket(bucket))
  }

  protected def bucketSlice(results: Seq[(Timestamp, LazyBucket[T])]): BucketSlice[T] = {
    BucketSlice[T](results.map(tuple ⇒ BucketResult(tuple._1, tuple._2)))
  }

  protected def uniqueTimestamps(buckets: Seq[T]): Seq[Timestamp] = {
    buckets.map(bucket ⇒ bucket.timestamp)
  }
}
