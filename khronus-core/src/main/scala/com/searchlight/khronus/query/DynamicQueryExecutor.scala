package com.searchlight.khronus.query

import com.searchlight.khronus.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait DynamicQueryExecutorSupport {
  def dynamicQueryExecutor: DynamicQueryExecutor = DynamicQueryExecutor.instance
}

object DynamicQueryExecutor {
  val instance = new DefaultDynamicQueryExecutor
}

trait DynamicQueryExecutor {
  def execute(query: DynamicQuery): Future[Seq[Series]]
}

class DefaultDynamicQueryExecutor extends DynamicQueryExecutor with BucketServiceSupport with QueryPlannerSupport {

  def execute(query: DynamicQuery): Future[Seq[Series]] = {
    val subMetricBucketsByQMetric = performBucketSlices(query, queryPlanner.getQueryPlan(query))
    executeProjections(query, subMetricBucketsByQMetric)
  }

  private def executeProjections(query: DynamicQuery, subMetricBucketsByQMetric: Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]]): Future[Seq[Series]] = {
    Future.sequence(query.projections.flatMap(projection ⇒ projection.execute(subMetricBucketsByQMetric))).map(_.flatten)
  }

  private def performBucketSlices(query: DynamicQuery, queryPlan: QueryPlan): Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]] = {
    val subMetricBucketsByQMetric = mapSubMetricsWithBuckets(queryPlan, retrieveBuckets(query, queryPlan))
    subMetricBucketsByQMetric
  }

  private def mapSubMetricsWithBuckets(queryPlan: QueryPlan, bucketsMap: Map[SubMetric, Future[BucketSlice[Bucket]]]): Map[QMetric, Map[SubMetric, Future[BucketSlice[Bucket]]]] = {
    queryPlan.subMetrics.mapValues { sm ⇒ sm.map(s ⇒ (s, bucketsMap(s))).toMap }
  }

  private def retrieveBuckets(query: DynamicQuery, selection: QueryPlan): Map[SubMetric, Future[BucketSlice[Bucket]]] = {
    selection.subMetrics.values.flatten.map(subMetric ⇒ execute(subMetric, query.range, query.resolution)).toMap
  }

  private def execute(subMetric: SubMetric, range: TimeRange, resolution: Option[Duration]): (SubMetric, Future[BucketSlice[Bucket]]) = {
    (subMetric, bucketService.retrieve(subMetric, range, resolution))
  }

}

case class DynamicQuery(projections: Seq[Projection], metrics: Seq[QMetric], predicate: Option[Predicate], range: TimeRange, resolution: Option[Duration] = None)

case class QMetric(name: String, alias: String)

case class TimeRange(from: Timestamp, to: Timestamp) {
  def mergedWith(other: TimeRange): TimeRange = {
    TimeRange(if (from.ms > other.from.ms) from else other.from, if (to.ms < other.to.ms) to else other.to)
  }
}

case class Series(name: String, points: Seq[Point])

case class Point(timestamp: Timestamp, value: Double)