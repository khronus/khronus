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

package com.despegar.khronus.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.commons.lang.builder.{EqualsBuilder, HashCodeBuilder}

import scala.concurrent.duration._
import org.HdrHistogram.Histogram

object MetricType {
  val Counter = "counter"
  val Timer = "timer"
  val Gauge = "gauge"
}

case class Metric(name: String, mtype: String, active: Boolean = true) {
  def isSystem = SystemMetric.isSystem(name)

  def canEqual(a: Any) = a.isInstanceOf[Metric]

  override def equals(that: Any): Boolean =
    that match {
      case that: Metric => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode:Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (name == null) 0 else name.hashCode);
    result = prime * result + (if (mtype == null) 0 else mtype.hashCode)
    return result
  }
}

object SystemMetric {
  val systemSymbol = '~'
  def isSystem(metricName: String) = {
    metricName.charAt(0) == systemSymbol
  }
}

case class MetricMeasurement(name: String, mtype: String, measurements: List[Measurement]) {

  override def toString = s"Metric($name,$mtype)"

  def asMetric = Metric(name, mtype)

  def asCounterBuckets = measurements.map(measurement ⇒ new CounterBucket(BucketNumber(measurement.ts, 1 millis), measurement.values.sum)).toSeq

}

case class Measurement(ts: Long, @JsonDeserialize(contentAs = classOf[java.lang.Long]) values: Seq[Long])

case class MetricBatch(metrics: List[MetricMeasurement])

