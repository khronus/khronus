package com.searchlight.khronus.model.query

import com.searchlight.khronus.model.Metric

trait Filter {
  def label: String

  def matches(metrics: Map[Selector, Metric]): Boolean
}

case class Equals(expression: Selector, tag: String, value: String) extends Filter {
  override val label = s"$tag=$value"

  def matches(metrics: Map[Selector, Metric]) = {
    metrics.get(expression)
      .exists(metric ⇒ metric.tags.exists(tagPair ⇒ tagPair._1.equals(tag) && tagPair._2.equals(value)))
  }
}

case class GreaterThan(alias: String, tag: String, value: Double) extends Filter {
  override val label = s"$tag>$value"

  def matches(metrics: Map[Selector, Metric]) = ???
}

case class MinorThan(alias: String, tag: String, value: Double) extends Filter {
  override val label = s"$tag<$value"

  def matches(metrics: Map[Selector, Metric]) = ???
}

case class In(selector: Selector, tag: String, values: List[String]) extends Filter {
  override val label = s"$tag=${values.mkString("|")}"

  def matches(metrics: Map[Selector, Metric]) = ???
}

case class And(filters: Seq[Filter]) extends Filter {
  override val label = s"${filters.map(_.label).mkString(",")}"

  def matches(metrics: Map[Selector, Metric]) = filters.forall(_.matches(metrics))
}

case class Or(filters: Seq[Filter]) extends Filter {
  override val label = s"${filters.map(_.label).mkString("|")}"

  def matches(metrics: Map[Selector, Metric]) = filters.exists(_.matches(metrics))
}

