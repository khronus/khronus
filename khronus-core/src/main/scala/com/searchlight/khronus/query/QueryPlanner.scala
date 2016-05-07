package com.searchlight.khronus.query

import com.searchlight.khronus.model.Metric
import com.searchlight.khronus.store.MetaSupport

trait QueryPlanner {
  def calculateQueryPlan(query: DynamicQuery): QueryPlan
}

trait QueryPlannerSupport {
  def queryPlanner: QueryPlanner = QueryPlanner.instance
}

object QueryPlanner {
  val instance = new DefaultQueryPlanner
}

class DefaultQueryPlanner extends QueryPlanner with MetaSupport {

  def calculateQueryPlan(query: DynamicQuery): QueryPlan = {
    val metrics = cartesianProduct(getQueriedMetrics(query)).map(_.toMap)
    val filteredMetrics = predicateFilter(query.predicate, metrics)
    QueryPlan(groupedByQMetric(filteredMetrics))
  }

  private def groupedByQMetric(filteredMetrics: Seq[Map[QMetric, Metric]]): Map[QMetric, Seq[Metric]] = {
    filteredMetrics.flatten.groupBy(kv ⇒ kv._1).mapValues(v ⇒ v.map(va ⇒ va._2))
  }

  private def predicateFilter(predicate: Option[Predicate], metrics: Seq[Map[QMetric, Metric]]): Seq[Map[QMetric, Metric]] = {
    predicate.map(p ⇒ metrics.filter(p.matches)) getOrElse metrics
  }

  private def getQueriedMetrics(query: DynamicQuery): Seq[Option[Seq[(QMetric, Metric)]]] = {
    query.metrics.map(qMetric ⇒ matchedMetrics(qMetric).map(metrics ⇒ metrics.map(metric ⇒ (qMetric, metric))))
  }

  private def matchedMetrics(qMetric: QMetric) = {
    val metricsMap = metaStore.getMetricsMap
    metricsMap.get(qMetric.name)
  }

  private def cartesianProduct[T](lists: Seq[Option[Seq[T]]]): Seq[Seq[T]] = {
    lists.filter(_.isDefined).map(_.get).foldLeft(Seq[Seq[T]]()) { (accum, list) ⇒
      accum match {
        case Nil ⇒ list.map(l ⇒ Seq(l))
        case _   ⇒ accum.flatMap(a ⇒ list.map(l ⇒ a :+ l))
      }
    }
  }
}

case class QueryPlan(metrics: Map[QMetric, Seq[Metric]])
