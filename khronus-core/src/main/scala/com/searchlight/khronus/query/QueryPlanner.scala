package com.searchlight.khronus.query

import com.searchlight.khronus.model.SubMetric
import com.searchlight.khronus.store.MetaSupport

trait QueryPlanner {
  def getQueryPlan(query: DynamicQuery): QueryPlan
}

trait QueryPlannerSupport {
  def queryPlanner: QueryPlanner = QueryPlanner.instance
}

object QueryPlanner {
  val instance = new DefaultQueryPlanner
}

class DefaultQueryPlanner extends QueryPlanner with MetaSupport {

  def getQueryPlan(query: DynamicQuery): QueryPlan = {
    val subMetrics = cartesianProduct(getQueriedSubMetrics(query)).map(_.toMap)
    val matchedSubMetrics = query.predicate map (p ⇒ subMetrics.filter(p.matches)) getOrElse Seq.empty //FIXME if none predicate?
    QueryPlan(matchedSubMetrics.flatten.groupBy(kv ⇒ kv._1).mapValues(v ⇒ v.map(va ⇒ va._2)))
  }

  private def getQueriedSubMetrics(query: DynamicQuery): Seq[Option[Seq[(QMetric, SubMetric)]]] = {
    query.metrics.map(qMetric ⇒ subMetricsOf(qMetric).map(s ⇒ s._2.map(sub ⇒ (qMetric, sub))))
  }

  private def subMetricsOf(qMetric: QMetric) = {
    metaStore.getMetricsMap.find(m ⇒ m._1.name.equals(qMetric.name))
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

case class QueryPlan(subMetrics: Map[QMetric, Seq[SubMetric]])
