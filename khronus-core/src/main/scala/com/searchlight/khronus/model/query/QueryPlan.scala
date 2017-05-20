package com.searchlight.khronus.model.query

import com.searchlight.khronus.model.Metric

case class QueryPlan(selectedMetrics: Map[Selector, Seq[Metric]]) {
  def metrics: Iterable[Metric] = selectedMetrics.values.flatten
}
