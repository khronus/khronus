package com.searchlight.khronus.service

import com.searchlight.khronus.model.Metric
import com.searchlight.khronus.model.query.{ Filter, Query, QueryPlan, Selector }
import com.searchlight.khronus.dao.{ Meta, MetaStore }

case class QueryPlanService(metaStore: MetaStore = Meta.metaStore) {

  def calculateQueryPlan(query: Query): QueryPlan = {
    val metrics = cartesianProduct(expandExpressions(query)).map(_.toMap)
    val filteredMetrics = predicateFilter(query.filter, metrics)
    QueryPlan(groupedBySelector(filteredMetrics))
  }

  private def groupedBySelector(filteredMetrics: Seq[Map[Selector, Metric]]): Map[Selector, Seq[Metric]] = {
    filteredMetrics.flatten.groupBy(kv ⇒ kv._1).mapValues(v ⇒ v.map(va ⇒ va._2))
  }

  private def predicateFilter(predicate: Option[Filter], metrics: Seq[Map[Selector, Metric]]): Seq[Map[Selector, Metric]] = {
    predicate.map(p ⇒ metrics.filter(p.matches)) getOrElse metrics
  }

  private def expandExpressions(query: Query): Seq[Option[Seq[(Selector, Metric)]]] = {
    query.selectors.map(expression ⇒ matchedMetrics(expression).map(metrics ⇒ metrics.map(metric ⇒ (expression, metric))))
  }

  private def matchedMetrics(expression: Selector) = {
    val metricsMap = metaStore.getMetricsMap
    metricsMap.get(expression.regex)
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
