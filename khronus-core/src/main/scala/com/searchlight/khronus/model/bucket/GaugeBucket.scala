package com.searchlight.khronus.model.bucket

import com.searchlight.khronus.model.summary.GaugeSummary
import com.searchlight.khronus.model.{ Bucket, BucketNumber }
import com.searchlight.khronus.util.Measurable

case class GaugeBucket(bucketNumber: BucketNumber, min: Long, max: Long, mean: Long, count: Long) extends Bucket {
  override def summary = GaugeSummary(timestamp, min, max, mean, count)
}

object GaugeBucket extends Measurable {
  implicit def aggregate(bucketNumber: BucketNumber, buckets: Seq[GaugeBucket]): GaugeBucket = {
    var min = Long.MaxValue
    var max = Long.MinValue
    var count = 0L
    var sum = 0L
    buckets.foreach { bucket ⇒
      count = count + 1
      sum = sum + bucket.mean
      min = if (bucket.min < min) bucket.min else min
      max = if (bucket.max < max) bucket.max else max
    }
    GaugeBucket(bucketNumber, min, max, sum / count, count)
  }
}
