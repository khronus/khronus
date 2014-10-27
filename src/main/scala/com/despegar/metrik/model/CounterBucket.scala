package com.despegar.metrik.model

import scala.concurrent.duration.Duration

class CounterBucket(override val bucketNumber: Long, override val duration: Duration, val counts: Long) extends Bucket(bucketNumber, duration) {

  override def summary: CounterSummary = {
    null
  }
}

object CounterBucket {
  implicit def sumCounters(buckets: Seq[CounterBucket]): Long = {
    buckets.foldLeft(0L)((accum, bucket) ⇒ accum + bucket.counts)
  }
}
