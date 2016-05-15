package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.bucket.{ HistogramBucket, CounterBucket, GaugeBucket }
import com.searchlight.khronus.query.{ FunctionProjection, Projection }
import net.sf.jsqlparser.expression.Expression

object Mean {
  def factory(alias: String, expressions: Seq[Expression]): Projection = {
    Mean(alias)
  }
}

case class Mean(alias: String = "") extends FunctionProjection {
  private val name = "mean"
  def values(bucket: CounterBucket) = Seq((name, bucket.counts.toDouble))

  def values(bucket: HistogramBucket) = Seq((name, bucket.histogram.getMean))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = Seq((name, bucket.mean.toDouble))
}