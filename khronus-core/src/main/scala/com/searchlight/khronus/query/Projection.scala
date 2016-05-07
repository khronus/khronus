package com.searchlight.khronus.query

import com.searchlight.khronus.api.{ Point, Series }
import com.searchlight.khronus.model._
import com.searchlight.khronus.model.bucket.{ CounterBucket, GaugeBucket, HistogramBucket }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait Projection {
  def execute(input: Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]]): Option[Future[Seq[Series]]]
}

trait FunctionProjection extends Projection {
  //TODO: refactor me
  def execute(input: Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]]): Option[Future[Seq[Series]]] = {
    targetQMetric(input).flatMap { qMetric ⇒
      input.get(qMetric).map { metrics ⇒
        bucketSlices(metrics).map { bucketSlices ⇒
          aggregate(metrics.keySet.head.mtype, groupedBuckets(bucketSlices)).toSeq.flatMap {
            case (bucketNumber, bucket) ⇒
              values(bucket).map(pair ⇒ (pair._1, Point(bucketNumber.startTimestamp().ms, pair._2)))
          }.groupBy {
            _._1
          }.map { case (projectionName, points) ⇒ Series(s"${qMetric.name}.$projectionName", points.map(_._2).sortBy(p ⇒ p.timestamp)) }.toSeq
        }
      }
    }
  }

  private def groupedBuckets(bucketSlices: Iterable[(Metric, BucketSlice[Bucket])]): Map[BucketNumber, Seq[BucketResult[Bucket]]] = {
    bucketSlices.flatMap(_._2.results).toSeq.groupBy(_.timestamp.toBucketNumberOf(1 minute))
  }

  private def bucketSlices(subMetrics: Map[Metric, Future[BucketSlice[Bucket]]]): Future[Iterable[(Metric, BucketSlice[Bucket])]] = {
    Future.sequence(subMetrics.map { case (subMetric, futures) ⇒ futures.map(buckets ⇒ (subMetric, buckets)) })
  }

  protected def values(bucket: Bucket): Seq[(String, Double)] = {
    bucket match {
      case counter: CounterBucket     ⇒ values(counter)
      case histogram: HistogramBucket ⇒ values(histogram)
      case gauge: GaugeBucket         ⇒ values(gauge)
    }
  }

  protected def values(gauge: GaugeBucket): Seq[(String, Double)]

  protected def values(bucket: CounterBucket): Seq[(String, Double)]

  protected def values(bucket: HistogramBucket): Seq[(String, Double)]

  private def aggregate(mtype: MetricType, bucketsMap: Map[BucketNumber, Seq[BucketResult[Bucket]]]): Map[BucketNumber, Bucket] = {
    bucketsMap.map { case (bucketNumber, buckets) ⇒ (bucketNumber, aggregate(mtype, bucketNumber, buckets)) }
  }

  private def aggregate(mtype: MetricType, number: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    mtype.aggregate(number, buckets)
  }

  private def targetQMetric(input: Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]]) = {
    input.find { case (qMetric, metrics) ⇒ alias.isEmpty || qMetric.alias.equals(alias) }.map(_._1)
  }

  def alias: String

}