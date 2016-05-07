package com.searchlight.khronus.query

import com.searchlight.khronus.model.Metric

trait Predicate {
  def matches(metrics: Map[QMetric, Metric]): Boolean
  protected def forAlias(alias: String, metrics: Map[QMetric, Metric]): Option[Metric] = {
    metrics.find { case (targetMetric, subMetric) ⇒ targetMetric.alias.equals(alias) }.map(k ⇒ k._2)
  }
}

case class Equals(alias: String, tag: String, value: String) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = {
    forAlias(alias, metrics)
      .exists(metric ⇒ metric.tags.exists(tagPair ⇒ tagPair._1.equals(tag) && tagPair._2.equals(value)))
  }
}

case class GreaterThan(alias: String, tag: String, value: Long) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = ???
}

case class MinorThan(alias: String, tag: String, value: Long) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = ???
}

case class In(alias: String, tag: String, values: List[String]) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = ???
}

case class And(a: Predicate, b: Predicate) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = a.matches(metrics) && b.matches(metrics)
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = a.matches(metrics) || b.matches(metrics)
}

case class SimpleExpression(alias: String) extends Predicate {
  def matches(metrics: Map[QMetric, Metric]) = forAlias(alias, metrics).isDefined
}