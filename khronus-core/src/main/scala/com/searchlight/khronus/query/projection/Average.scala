package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.bucket.{ HistogramBucket, CounterBucket, GaugeBucket }
import com.searchlight.khronus.query.{ FunctionProjection, Projection }
import net.sf.jsqlparser.expression.Expression

object Average {
  def factory(alias: String, expressions: Seq[Expression]): Projection = {
    Average(alias)
  }
}

case class Average(alias: String = "") extends FunctionProjection {
  private val name = "average"
  def values(bucket: CounterBucket) = Seq((name, bucket.counts.toDouble))

  def values(bucket: HistogramBucket) = Seq((name, bucket.histogram.getMean))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = Seq((name, bucket.average.toDouble))
}