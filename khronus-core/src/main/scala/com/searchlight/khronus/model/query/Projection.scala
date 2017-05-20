package com.searchlight.khronus.model.query

import com.searchlight.khronus.model.bucket.{ HistogramBucket, CounterBucket }
import com.searchlight.khronus.model.summary.{ HistogramSummary, CounterSummary }
import com.searchlight.khronus.model.{ Bucket, Summary }

import scala.concurrent.duration.Duration

sealed trait Projection {
  def value(bucket: Bucket): Double

  def value(summary: Summary): Double

  def label: String
}

sealed trait Function extends Projection {
  def selector: Selector
}

sealed trait Operator extends Projection {
  def a: Projection

  def b: Projection

  def calculation(a: Double, b: Double): Double
}

case class Number(n: Double) extends Projection {
  override def value(bucket: Bucket): Double = n

  override def value(summary: Summary): Double = n

  override val label = n.toString
}

case class Count(selector: Selector, rate: Option[Duration] = None) extends Function {
  private def ratedFraction(bucket: Bucket): Double = rate.map(_ / bucket.bucketNumber.duration).getOrElse(1d)

  override def value(bucket: Bucket): Double = bucket match {
    case c: CounterBucket   ⇒ c.value.toDouble * ratedFraction(bucket)
    case h: HistogramBucket ⇒ h.histogram.getTotalCount.toDouble * ratedFraction(bucket)
  }

  override def value(summary: Summary): Double = summary match {
    case c: CounterSummary   ⇒ c.count.toDouble
    case h: HistogramSummary ⇒ h.count.toDouble
  }

  override val label = "count"
}

case class Percentile(selector: Selector, percentile: Double) extends Function {

  override def value(bucket: Bucket): Double = bucket match {
    case c: CounterBucket   ⇒ throw new UnsupportedOperationException
    case h: HistogramBucket ⇒ h.histogram.getValueAtPercentile(percentile)
  }

  override def value(summary: Summary): Double = summary match {
    case c: CounterSummary   ⇒ ???
    case h: HistogramSummary ⇒ ???
  }

  override val label = s"$percentile%"
}

case class Average(selector: Selector) extends Function {
  override val label = "average"

  override def value(bucket: Bucket): Double = bucket match {
    case c: CounterBucket   ⇒ c.value.toDouble
    case h: HistogramBucket ⇒ h.histogram.getMean
  }

  override def value(summary: Summary): Double = ???
}

case class Plus(a: Projection, b: Projection) extends Operator {
  override def value(bucket: Bucket): Double = ???

  override def value(summary: Summary): Double = ???

  override def calculation(a: Double, b: Double) = a + b

  override val label = "+"
}

case class Minus(a: Projection, b: Projection) extends Operator {
  override def value(bucket: Bucket): Double = ???

  override def value(summary: Summary): Double = ???

  override def calculation(a: Double, b: Double) = a - b

  override val label = "-"
}

case class Div(a: Projection, b: Projection) extends Operator {
  override def value(bucket: Bucket): Double = ???

  override def value(summary: Summary): Double = ???

  override def calculation(a: Double, b: Double) = a / b

  override val label = "/"
}

case class Multiply(a: Projection, b: Projection) extends Operator {
  override def value(bucket: Bucket): Double = ???

  override def value(summary: Summary): Double = ???

  override def calculation(a: Double, b: Double) = a * b

  override val label = "*"
}