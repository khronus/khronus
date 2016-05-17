package com.searchlight.khronus.query.projection

import com.searchlight.khronus.model.Bucket
import com.searchlight.khronus.model.bucket.{ CounterBucket, HistogramBucket, GaugeBucket }
import com.searchlight.khronus.query.{ FunctionProjection, Projection }
import net.sf.jsqlparser.expression.Expression

import scala.concurrent.duration.Duration

object Count {
  def factory(alias: String, expressions: Seq[Expression]): Projection = {
    alias match {
      case "value" ⇒ Count()
      case _       ⇒ Count(alias)
    }
  }
}

case class Count(alias: String = "", rate: Option[Duration] = None) extends FunctionProjection {
  private val name = "count"

  def values(bucket: CounterBucket) = Seq((name, bucket.counts.toDouble * ratedFraction(bucket)))

  def values(bucket: HistogramBucket) = Seq((name, bucket.histogram.getTotalCount.toDouble * ratedFraction(bucket)))

  def values(bucket: GaugeBucket): Seq[(String, Double)] = Seq((name, bucket.count.toDouble * ratedFraction(bucket)))

  private def ratedFraction(bucket: Bucket): Double = rate.map(_ / bucket.bucketNumber.duration).getOrElse(1d)
}
