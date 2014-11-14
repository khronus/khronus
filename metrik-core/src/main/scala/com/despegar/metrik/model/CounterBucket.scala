package com.despegar.metrik.model

class CounterBucket(bucketNumber: BucketNumber, val counts: Long) extends Bucket(bucketNumber) {

  override def summary = CounterSummary(timestamp, counts)

}

object CounterBucket {
  implicit def sumCounters(buckets: Seq[CounterBucket]): Long = buckets.map(_.counts).sum
}
