package com.despegar.metrik.model

import scala.concurrent.duration.Duration

class CounterBucket(override val bucketNumber: BucketNumber, val counts: Long) extends Bucket(bucketNumber) {

  override def summary = CounterSummary(timestamp, counts)

}

object CounterBucket {
  implicit def sumCounters(buckets: Seq[CounterBucket]): Long = buckets.map(_.counts).sum
}
