package com.searchlight.khronus.query

import com.searchlight.khronus.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait Projection {
  def execute(input: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]): Option[Future[Seq[Series]]] = {
    targetQMetric(input).flatMap { qMetric ⇒
      input.get(qMetric).map { subMetrics ⇒
        bucketSlices(subMetrics).map { bucketSlices ⇒
          aggregate(subMetrics.keySet.head.asMetric(), groupedBuckets(bucketSlices)).toSeq.flatMap {
            case (bucketNumber, bucket) ⇒
              values(bucket).map(pair ⇒ (pair._1, Point(bucketNumber.startTimestamp(), pair._2)))
          }.groupBy {
            _._1
          }.map { case (projectionName, points) ⇒ Series(s"${qMetric.name}.$projectionName", points.map(_._2)) }.toSeq
        }
      }
    }
  }

  private def groupedBuckets(bucketSlices: Iterable[(SubMetric, BucketSlice[Bucket])]): Map[BucketNumber, Seq[BucketResult[Bucket]]] = {
    bucketSlices.flatMap(_._2.results).toSeq.groupBy(_.timestamp.toBucketNumberOf(1 minute))
  }

  private def bucketSlices(subMetrics: Map[SubMetric, Future[BucketSlice[Bucket]]]): Future[Iterable[(SubMetric, BucketSlice[Bucket])]] = {
    Future.sequence(subMetrics.map { case (subMetric, futures) ⇒ futures.map(buckets ⇒ (subMetric, buckets)) })
  }

  protected def values(bucket: Bucket): Seq[(String, Double)] = {
    bucket match {
      case counter: CounterBucket     ⇒ values(counter)
      case histogram: HistogramBucket ⇒ values(histogram)
    }
  }

  protected def values(bucket: CounterBucket): Seq[(String, Double)]

  protected def values(bucket: HistogramBucket): Seq[(String, Double)]

  private def aggregate(metric: Metric, bucketsMap: Map[BucketNumber, Seq[BucketResult[Bucket]]]): Map[BucketNumber, Bucket] = {
    bucketsMap.map { case (bucketNumber, buckets) ⇒ (bucketNumber, aggregate(metric, bucketNumber, buckets)) }
  }

  private def aggregate(metric: Metric, number: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    metric.mtype match {
      case "counter"                       ⇒ new CounterBucket(number, CounterBucket.sumCounters(buckets.map(_.lazyBucket().asInstanceOf[CounterBucket])))
      case "timer" | "histogram" | "gauge" ⇒ new HistogramBucket(number, HistogramBucket.sumHistograms(buckets.map(_.lazyBucket().asInstanceOf[HistogramBucket])))
    }
  }

  private def targetQMetric(input: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]) = {
    input.find { case (qMetric, subMetrics) ⇒ qMetric.alias.equals(alias) }.map(_._1)
  }

  def subMetrics(input: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]): Option[Map[SubMetric, Future[BucketSlice[Bucket]]]] = {
    input.find { case (qMetric, subMetrics) ⇒ qMetric.alias.equals(alias) }.map(_._2)
  }

  def alias: String

}

case class Count(alias: String) extends Projection {
  def values(counter: CounterBucket) = Seq(("count", counter.counts.toDouble))

  def values(histogram: HistogramBucket) = Seq(("count", histogram.histogram.getTotalCount.toDouble))
}

case class Percentiles(alias: String, percentiles: Seq[Double]) extends Projection {
  def values(counter: CounterBucket) = percentiles.map(p ⇒ ("undefined", 0d))

  def values(histogram: HistogramBucket) = percentiles.map(percentile ⇒ (s"p$percentile", histogram.histogram.getValueAtPercentile(percentile).toDouble))
}
