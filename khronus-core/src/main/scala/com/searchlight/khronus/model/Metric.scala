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
import com.searchlight.khronus.model.bucket.{ CounterBucket, GaugeBucket, HistogramBucket }
import com.searchlight.khronus.store.CassandraMetricMeasurementStore._
import org.HdrHistogram.{ Histogram ⇒ HdrHistogram }

object MetricSpecs {
  sealed trait MetricSpec
  case object Dimensional extends MetricSpec
  case object NonDimensional extends MetricSpec
}

object MetricType {
  private val map = Map(Counter.toString -> Counter, "timer" -> Histogram, Histogram.toString -> Histogram, Gauge.toString -> Gauge)

  implicit def fromStringToMetricType(typeName: String): MetricType = map.getOrElse(typeName, throw new RuntimeException(s"Unknown metric type $typeName"))

  implicit def fromMetricTypeToString(metricType: MetricType): String = metricType.toString
}

sealed trait MetricType {
  def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measurements: List[Measurement]): Bucket

  def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket

  protected def skipNegativeValues(metric: Metric, values: Seq[Long]): Seq[Long] = {
    val (negativeValues, positiveValues) = values.partition(value ⇒ value < 0)
    if (negativeValues.nonEmpty)
      log.warn(s"Skipping negative values for metric $metric: $negativeValues")
    positiveValues
  }
}

case object Counter extends MetricType {
  override val toString = "counter"

  override def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measures: List[Measurement]): Bucket = {
    val count = measures.map(measure ⇒ skipNegativeValues(metric, measure.values).sum).sum
    CounterBucket(bucketNumber, count)
  }

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    CounterBucket(bucketNumber, CounterBucket.aggregate(buckets.map(_.lazyBucket().asInstanceOf[CounterBucket])))
  }
}

case object Gauge extends MetricType {
  override val toString = "gauge"

  override def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measures: List[Measurement]): Bucket = {
    var min = Long.MaxValue
    var max = Long.MinValue
    var count = 0L
    var sum = 0L
    measures.foreach { measure ⇒
      count = count + measure.values.size
      sum = sum + measure.values.sum
      min = if (measure.values.min < min) measure.values.min else min
      max = if (measure.values.max > max) measure.values.max else max
    }
    GaugeBucket(bucketNumber, min, max, sum / count, count)
  }

  override def aggregate(bucketNumber: BucketNumber, buckets: Seq[BucketResult[Bucket]]): Bucket = {
    GaugeBucket.aggregate(bucketNumber, buckets.map(_.lazyBucket().asInstanceOf[GaugeBucket]))
  }
}

case object Histogram extends MetricType {
  override val toString = "histogram"

  override def bucketWithMeasures(metric: Metric, bucketNumber: BucketNumber, measures: List[Measurement]): Bucket = {
    val histogram = HistogramBucket.newHistogram(maxValue(measures))
    measures.foreach(measure ⇒ record(metric, measure, histogram))
    HistogramBucket(bucketNumber, histogram)
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
    HistogramBucket.aggregate(bucketNumber, buckets.map(_.lazyBucket().asInstanceOf[HistogramBucket]))
  }
}

case class Metric(name: String, mtype: MetricType, tags: Map[String, String] = Map()) {

  def isSystem = SystemMetric.isSystem(name)

  def flatName = {
    if (tags.nonEmpty) {
      s"$name[${flattenTags()}]"
    } else
      name
  }

  private def flattenTags(): String = {
    tags.keys.toSeq.sorted.map(key ⇒ s"$key:${tags(key)}").mkString(",")
  }

}

object Metric {
  private val patternHasTags = "[^\\[]+\\[(.+)\\]$".r
  // anything but a '[', then optionally a '[' that must by closed by ']' to the end of the string
  private val patternForMetricsWithTags = "([^\\[]+)(?:\\[(.+)\\])?$" .r
  private val tagsPattern = "((\\w+))".r

  def fromFlatNameToMetric(flatName: String, mtype: MetricType): Metric = {
    if (patternHasTags.findAllMatchIn(flatName).toArray.length > 0 ) {
      val patternForMetricsWithTags(metricName, tagsString) = flatName
      if (tagsString != null) {
        val tags = tagsPattern.findAllIn(tagsString).grouped(2).map(group ⇒ group.head -> group.last).toMap
        return Metric(metricName, mtype, tags)
      }
    }

    Metric(flatName, mtype, Map())
  }
}

object SystemMetric {
  val systemSymbol = '~'

  def isSystem(metricName: String) = {
    metricName.charAt(0) == systemSymbol
  }
}