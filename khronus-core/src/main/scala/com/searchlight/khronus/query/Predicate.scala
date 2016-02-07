package com.searchlight.khronus.query

import com.searchlight.khronus.model.SubMetric

trait Predicate {
  def matches(metrics: Map[QMetric, SubMetric]): Boolean
  protected def forAlias(alias: String, metrics: Map[QMetric, SubMetric]): Option[SubMetric] = {
    metrics.find { case (targetMetric, subMetric) ⇒ targetMetric.alias.equals(alias) }.map(k ⇒ k._2)
  }
}

case class Equals(alias: String, tag: String, value: String) extends Predicate {
  def matches(metrics: Map[QMetric, SubMetric]) = {
    forAlias(alias, metrics)
      .exists(metric ⇒ metric.tags.exists(tagPair ⇒ tagPair._1.equals(tag) && tagPair._2.equals(value)))
  }
}

case class And(a: Predicate, b: Predicate) extends Predicate {
  def matches(metrics: Map[QMetric, SubMetric]) = a.matches(metrics) && b.matches(metrics)
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  def matches(metrics: Map[QMetric, SubMetric]) = a.matches(metrics) || b.matches(metrics)
}

case class SimpleExpression(alias: String) extends Predicate {
  def matches(metrics: Map[QMetric, SubMetric]) = forAlias(alias, metrics).isDefined
}