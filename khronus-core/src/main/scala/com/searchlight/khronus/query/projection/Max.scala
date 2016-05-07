package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.bucket.{ GaugeBucket, HistogramBucket, CounterBucket }
import com.searchlight.khronus.query.{ FunctionProjection, Projection }
import net.sf.jsqlparser.expression.Expression

object Max {
  def factory(alias: String, expressions: Seq[Expression]): Projection = Max(alias)
}

case class Max(alias: String = "") extends FunctionProjection {
  private val name = "max"
  def values(bucket: CounterBucket) = Seq((name, bucket.counts.toDouble))

  def values(bucket: HistogramBucket) = Seq((name, bucket.histogram.getMaxValue.toDouble))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = Seq((name, bucket.max.toDouble))
}