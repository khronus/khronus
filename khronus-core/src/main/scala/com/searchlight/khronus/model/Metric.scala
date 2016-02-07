package com.searchlight.khronus.model

object MetricType {
  val Counter = "counter"
  val Timer = "timer"
  val Gauge = "gauge"
}

sealed trait MetricType
sealed trait HistogramType extends MetricType
case object KCounter extends MetricType
case object KTimer extends HistogramType
case object KHistogram extends HistogramType
case object KGauge extends HistogramType

case class Metric(name: String, mtype: String) {
  private val pattern = "([\\w]*)\\[?([\\w:,]*)\\]?".r
  private val tagsPattern = "((\\w+))".r
  def isSystem = SystemMetric.isSystem(name)

  private def extractTags(): (String, Map[String, String]) = {
    val pattern(metricName, tagsString) = name
    val tags = tagsPattern.findAllIn(tagsString).grouped(2).map(group ⇒ group.head -> group.last).toMap
    (metricName, tags)
  }

  def asSubMetric() = {
    val (name, tags) = extractTags()
    SubMetric(Metric(name, mtype), tags)
  }
}

case class SubMetric(metric: Metric, tags: Map[String, String]) {
  def asMetric() = {
    if (tags.nonEmpty) {
      Metric(s"${metric.name}[${flattenTags()}]", metric.mtype)
    } else
      metric
  }
  private def flattenTags(): String = {
    tags.keys.toSeq.sorted.map(key ⇒ s"$key:${tags(key)}").mkString(",")
  }
}

object SystemMetric {
  val systemSymbol = '~'
  def isSystem(metricName: String) = {
    metricName.charAt(0) == systemSymbol
  }
}