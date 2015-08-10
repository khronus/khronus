package com.searchlight.khronus.model

import com.searchlight.khronus.util.Measurable

class CounterBucket(bucketNumber: BucketNumber, val counts: Long) extends Bucket(bucketNumber) {

  override def summary = CounterSummary(timestamp, counts)

}

object CounterBucket extends Measurable {
  implicit def sumCounters(buckets: Seq[CounterBucket]): Long = buckets.map(_.counts).sum
}
