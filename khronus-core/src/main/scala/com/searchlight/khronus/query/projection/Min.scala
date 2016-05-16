package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.bucket.{ GaugeBucket, HistogramBucket, CounterBucket }
import com.searchlight.khronus.query.FunctionProjection

case class Min(alias: String = "") extends FunctionProjection {
  private val name = "min"
  def values(bucket: CounterBucket) = Seq((name, bucket.counts.toDouble))

  def values(bucket: HistogramBucket) = Seq((name, bucket.histogram.getMinValue.toDouble))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = Seq((name, bucket.min.toDouble))
}
