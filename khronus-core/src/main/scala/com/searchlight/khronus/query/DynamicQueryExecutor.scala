package com.searchlight.khronus.query

import com.searchlight.khronus.api.Series
import com.searchlight.khronus.model._
import com.searchlight.khronus.util.Settings

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
    val queryPlan = queryPlanner.calculateQueryPlan(query)
    val bucketsByMetric = retrieveBuckets(query, queryPlan)
    val bucketsByQMetric = mapQMetricsWithBuckets(queryPlan, bucketsByMetric)
    executeProjections(query, bucketsByQMetric)
  }

  private def executeProjections(query: DynamicQuery, bucketsByQMetric: Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]]): Future[Seq[Series]] = {
    Future.sequence(query.projections.flatMap(projection ⇒ projection.execute(bucketsByQMetric))).map(_.flatten)
  }

  private def mapQMetricsWithBuckets(queryPlan: QueryPlan, bucketsMap: Map[Metric, Future[BucketSlice[Bucket]]]): Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]] = {
    queryPlan.metrics.mapValues { sm ⇒ sm.map(s ⇒ (s, bucketsMap(s))).toMap }
  }

  private def retrieveBuckets(query: DynamicQuery, selection: QueryPlan): Map[Metric, Future[BucketSlice[Bucket]]] = {
    selection.metrics.values.flatten.map(metric ⇒ execute(metric, query.slice, query.resolution)).toMap
  }

  private def execute(metric: Metric, slice: Slice, resolution: Option[Duration]): (Metric, Future[BucketSlice[Bucket]]) = {
    (metric, bucketService.retrieve(metric, slice, resolution))
  }

}

case class DynamicQuery(projections: Seq[Projection], metrics: Seq[QMetric], predicate: Option[Predicate], slice: Slice, resolution: Option[Duration] = None)

case class QMetric(name: String, alias: String)

case class Slice(from: Long = -1L, to: Long) {

  def mergedWith(other: Slice): Slice = {
    Slice(if (from > other.from) from else other.from, if (to < other.to) to else other.to)
  }

  def getAdjustedResolution(resolution: Duration, forceResolution: Boolean, minResolution: Int, maxResolution: Int): Duration = {
    val sortedWindows = Settings.Window.ConfiguredWindows.toSeq.sortBy(_.toMillis).reverse
    val nearestConfiguredWindow = sortedWindows.foldLeft(sortedWindows.last)((nearest, next) ⇒ if (millisBetween(resolution, next) < millisBetween(resolution, nearest)) next else nearest)

    if (forceResolution) {
      nearestConfiguredWindow
    } else {
      val points = resolutionPoints(nearestConfiguredWindow)
      if (points <= maxResolution & points >= minResolution)
        nearestConfiguredWindow
      else {
        sortedWindows.foldLeft(sortedWindows.head)((adjustedWindow, next) ⇒ {
          val points = resolutionPoints(next)
          if (points >= minResolution & points <= maxResolution)
            next
          else if (points < minResolution) next else adjustedWindow
        })
      }
    }
  }

  private def millisBetween(some: Duration, other: Duration) = Math.abs(some.toMillis - other.toMillis)

  private def resolutionPoints(timeWindow: Duration) = {
    Math.abs(to - from) / timeWindow.toMillis
  }
}
