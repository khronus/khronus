/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */
package com.searchlight.khronus.model

import com.searchlight.khronus.api.Measurement
import com.searchlight.khronus.store.CassandraMetricMeasurementStore._
import org.HdrHistogram.{ Histogram ⇒ HdrHistogram }

object MetricType {
  private val map = Map("counter" -> Counter, "timer" -> Histogram, "histogram" -> Histogram, "gauge" -> Histogram)
  implicit def fromStringToMetricType(typeName: String): MetricType = map.getOrElse(typeName, throw new RuntimeException(s"Unknown metric type $typeName"))
  implicit def fromMetricTypeToString(metricType: MetricType): String = metricType.toString
}

sealed trait MetricType {
  def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measurements: List[Measurement]): Bucket
  def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket
  protected def skipNegativeValues(metric: Metric, values: Seq[Long]): Seq[Long] = {
    val (invalidValues, okValues) = values.partition(value ⇒ value < 0)
    if (invalidValues.nonEmpty)
      log.warn(s"Skipping invalid values for metric $metric: $invalidValues")
    okValues
  }
}
case object Counter extends MetricType {
  override val toString = "counter"
  override def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measures: List[Measurement]): Bucket = {
    val counts = measures.map(measure ⇒ skipNegativeValues(metric, measure.values).sum).sum
    new CounterBucket(bucketNumber, counts)
  }

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    new CounterBucket(bucketNumber, CounterBucket.sumCounters(buckets.map(_.lazyBucket().asInstanceOf[CounterBucket])))
  }
}
case object Histogram extends MetricType {
  override val toString = "histogram"

  override def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measures: List[Measurement]): Bucket = {
    val histogram = HistogramBucket.newHistogram(maxValue(measures))
    measures.foreach(measure ⇒ record(metric, measure, histogram))
    new HistogramBucket(bucketNumber, histogram)
  }

  private def maxValue(measurements: List[Measurement]) = {
    var maxValue = 0L
    measurements.foreach { measurement ⇒
      if (measurement.values.nonEmpty) {
        val value = measurement.values.max
        if (value > maxValue) {
          maxValue = value
        }
      }
    }
    maxValue
  }

  private def record(metric: Metric, measure: Measurement, histogram: HdrHistogram): Unit = {
    skipNegativeValues(metric, measure.values).foreach(value ⇒ {
      val highestTrackableValue = histogram.getHighestTrackableValue
      if (value <= highestTrackableValue) histogram.recordValue(value)
      else {
        val exceeded = value - highestTrackableValue
        log.warn(s"Sample of $metric has exceeded the highestTrackableValue of $highestTrackableValue by $exceeded. Truncating the excedent. Try changing the sampling unit or increasing the highestTrackableValue")
        histogram.recordValue(highestTrackableValue)
      }
    })
  }

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    new HistogramBucket(bucketNumber, HistogramBucket.sumHistograms(buckets.map(_.lazyBucket().asInstanceOf[HistogramBucket])))
  }
}

case class Metric(name: String, mtype: MetricType) {
  import Metric._
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

object Metric {
  private val pattern = "([~\\-\\_\\.\\w]*)\\[?([\\w:,]*)\\]?".r
  private val tagsPattern = "((\\w+))".r
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