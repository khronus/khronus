package com.searchlight.khronus.service

import com.searchlight.khronus.model._
import com.searchlight.khronus.model.query._

import scala.collection.immutable.TreeMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class BucketQueryService(bucketService: BucketService = CassandraBucketService(), queryPlanner: QueryPlanService = QueryPlanService(),
    projectionService: ProjectionService = ProjectionService()) {

  def execute(query: Query): Future[Seq[Series]] = {
    val queryPlan = queryPlanner.calculateQueryPlan(query)
    val bucketsByMetric = retrieveBuckets(query, queryPlan)
    val bucketsBySelector = getBucketsBySelector(queryPlan, bucketsByMetric)
    executeProjections(query, bucketsBySelector)
  }

  private def executeProjections(query: Query, bucketsByExpression: Map[Selector, Map[Metric, Future[BucketSlice[Bucket]]]]): Future[Seq[Series]] = {
    projectionService.calculate(query, grouped(bucketsByExpression, query))
  }

  private def groupKey(query: Query, selector: Selector, metric: Metric) = {
    query.group.get(selector).map { tags ⇒
      tags.map(tag ⇒ (tag, metric.tags.get(tag))).toMap
    }
  }

  private def grouped(bucketsBySelector: Map[Selector, Map[Metric, Future[BucketSlice[Bucket]]]], query: Query) = {
    groupMetricsByDimensions(query, bucketsBySelector).map {
      case ((selector, dimensions), values) ⇒
        val metrics: Seq[Metric] = values.map(value ⇒ value._2)
        val mtype = metrics.head.mtype
        Group(selector, dimensions, aggregateBuckets(query, mtype, bucketsBySelector(selector), metrics))
    }.toSeq
  }

  private def groupMetricsByDimensions(query: Query, bucketsBySelector: Map[Selector, Map[Metric, Future[BucketSlice[Bucket]]]]): Map[(Selector, Map[String, String]), Seq[(Selector, Metric)]] = {
    val selectorToMetric: Seq[(Selector, Metric)] = bucketsBySelector.map {
      case (selector, bucketsByMetric) ⇒
        bucketsByMetric.keySet.map { metric ⇒ (selector, metric) }
    }.toSeq.flatten
    selectorToMetric.groupBy {
      case (selector, metric) ⇒
        (selector, getDimensions(groupKey(query, selector, metric)))
    }
  }

  private def getDimensions(groupKey: Option[Map[String, Option[String]]]): Map[String, String] = {
    groupKey.getOrElse(Map()).filter { case (key, valueOption) ⇒ valueOption.isDefined }.mapValues(_.get)
  }

  private def aggregateBuckets(query: Query, mtype: MetricType, bucketsByMetric: Map[Metric, Future[BucketSlice[Bucket]]], metrics: Seq[Metric]): Future[TreeMap[BucketNumber, LazyBucket[Bucket]]] = {
    val resolution: Duration = query.resolution.get
    val buckets = Future.sequence(metrics.map(metric ⇒ bucketsByMetric(metric))).map {
      slices ⇒
        slices.flatMap(_.results).groupBy(_.timestamp.toBucketNumberOf(resolution))
          .map {
            case (bucketNumber, results) ⇒
              (bucketNumber, new LazyBucket(mtype.aggregate(bucketNumber, results.map(result ⇒ result.lazyBucket))))
          }
    }.map { aggregatedBuckets ⇒
      TreeMap(aggregatedBuckets.toSeq: _*)(Ordering.by({ bucketNumber ⇒ bucketNumber.number }))
    }
    buckets
  }

  private def getBucketsBySelector(queryPlan: QueryPlan, bucketsMap: Map[Metric, Future[BucketSlice[Bucket]]]): Map[Selector, Map[Metric, Future[BucketSlice[Bucket]]]] = {
    queryPlan.selectedMetrics.mapValues { metrics ⇒ metrics.map(metric ⇒ (metric, bucketsMap(metric))).toMap }
  }

  private def retrieveBuckets(query: Query, queryPlan: QueryPlan): Map[Metric, Future[BucketSlice[Bucket]]] = {
    queryPlan.metrics.map(metric ⇒ execute(metric, query.timeRange.get, query.resolution.get)).toMap
  }

  private def execute(metric: Metric, timeRange: TimeRange, resolution: Duration): (Metric, Future[BucketSlice[Bucket]]) = {
    (metric, bucketService.retrieve(metric, timeRange, resolution))
  }

}
