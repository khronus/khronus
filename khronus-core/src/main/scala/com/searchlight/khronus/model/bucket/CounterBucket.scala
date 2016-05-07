package com.searchlight.khronus.model.bucket

import com.searchlight.khronus.model.summary.CounterSummary
import com.searchlight.khronus.model.{ Bucket, BucketNumber }
import com.searchlight.khronus.util.Measurable

case class CounterBucket(bucketNumber: BucketNumber, counts: Long) extends Bucket {
  override def summary = CounterSummary(timestamp, counts)
}

object CounterBucket extends Measurable {
  implicit def aggregate(buckets: Seq[CounterBucket]): Long = buckets.map(_.counts).sum
}
