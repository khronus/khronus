package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.bucket.{ HistogramBucket, CounterBucket, GaugeBucket }
import com.searchlight.khronus.query.{ FunctionProjection, Projection }
import net.sf.jsqlparser.expression.{ DoubleValue, Expression, LongValue }

object Percentiles {
  def factory(alias: String, expressions: Seq[Expression]): Projection = {
    Percentiles(alias, expressions.map { expression ⇒
      expression match {
        case longValue: LongValue     ⇒ longValue.getValue.toDouble
        case doubleValue: DoubleValue ⇒ doubleValue.getValue
      }
    })
  }
}

case class Percentiles(alias: String, percentiles: Seq[Double]) extends FunctionProjection {
  private val undefined: Seq[(String, Double)] = percentiles.map(p ⇒ ("undefined", 0d))

  def values(bucket: CounterBucket) = undefined

  def values(bucket: HistogramBucket) = percentiles.map(percentile ⇒ (s"p$percentile", bucket.histogram.getValueAtPercentile(percentile).toDouble))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = undefined
}

