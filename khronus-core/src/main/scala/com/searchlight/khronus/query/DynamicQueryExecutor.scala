package com.searchlight.khronus.query

import com.searchlight.khronus.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DynamicQuerySupport {
  val dynamicQueryExecutor: DynamicQueryExecutor = new DefaultDynamicQueryExecutor()
}

trait DynamicQueryExecutor {
  def execute(query: DynamicQuery): Future[Seq[Series]]
}

class DefaultDynamicQueryExecutor() extends DynamicQueryExecutor with BucketQuerySupport with QueryPlannerSupport {

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
    selection.subMetrics.values.flatten.map(subMetric ⇒ execute(subMetric, query.range)).toMap
  }

  private def execute(subMetric: SubMetric, range: TimeRange): (SubMetric, Future[BucketSlice[Bucket]]) = {
    (subMetric, bucketQueryExecutor.retrieve(subMetric, range))
  }

}

case class DynamicQuery(projections: Seq[Projection], metrics: Seq[QMetric], predicate: Predicate, range: TimeRange)

case class QMetric(name: String, alias: String)

case class TimeRange(from: Timestamp, to: Timestamp)

case class Series(name: String, points: Seq[Point])

case class Point(timestamp: Timestamp, value: Double)
