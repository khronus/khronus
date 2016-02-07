package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait Projection {
  def execute(input: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]): Option[Future[Seq[Series]]] = {
    subMetrics(input).map { subMetrics ⇒
      val res = Future.sequence(subMetrics.map { case (subMetric, futures) ⇒ futures.map(buckets ⇒ (subMetric, buckets)) })
      res.map { entries ⇒
        aggregate(entries.flatMap(_._2.results).toSeq.groupBy(_.timestamp.toBucketNumberOf(1 minute))).toSeq.map {
          kv ⇒
            value(kv._2).map(value ⇒ Point(kv._1.startTimestamp(), value))
        }.map(points ⇒ Series(alias, points))
      }
    }
  }

  protected def value(bucket: Bucket): Seq[Double] = {
    bucket match {
      case counter: CounterBucket     ⇒ value(counter)
      case histogram: HistogramBucket ⇒ value(histogram)
    }
  }

  protected def value(bucket: CounterBucket): Seq[Double]

  protected def value(bucket: HistogramBucket): Seq[Double]

  private def aggregate(bucketsMap: Map[BucketNumber, Seq[BucketResult[Bucket]]]): Map[BucketNumber, Bucket] = {
    bucketsMap.map { case (bucketNumber, buckets) ⇒ (bucketNumber, aggregate(bucketNumber, buckets)) }
  }

  private def aggregate(number: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    buckets match {
      case counters: Seq[BucketResult[CounterBucket]]     ⇒ new CounterBucket(number, CounterBucket.sumCounters(counters.map(_.lazyBucket())))
      case histograms: Seq[BucketResult[HistogramBucket]] ⇒ new HistogramBucket(number, HistogramBucket.sumHistograms(histograms.map(_.lazyBucket())))
    }
  }

  def subMetrics(input: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]): Option[Map[SubMetric, Future[BucketSlice[Bucket]]]] = {
    input.find { case (qMetric, subMetrics) ⇒ qMetric.alias.equals(alias) }.map(_._2)
  }

  def alias: String

}

case class Count(alias: String) extends Projection {
  def value(counter: CounterBucket) = Seq(counter.counts)

  def value(histogram: HistogramBucket) = Seq(histogram.histogram.getTotalCount)
}

case class Percentiles(alias: String, percentiles: Seq[Double]) extends Projection {
  def value(counter: CounterBucket) = percentiles.map(p ⇒ 0d)

  def value(histogram: HistogramBucket) = percentiles.map(percentile ⇒ histogram.histogram.getValueAtPercentile(percentile).toDouble)
}
