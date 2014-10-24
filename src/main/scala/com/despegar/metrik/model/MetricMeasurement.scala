/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.model

import spray.json.DefaultJsonProtocol
import spray.httpx.SprayJsonSupport
import spray.json.CollectionFormats
import scala.concurrent.duration._
import org.HdrHistogram.Histogram
import spray.json.RootJsonFormat
import spray.json._

case class Metric(name: String, mtype: String)

case class MetricMeasurement(name: String, mtype: String, measurements: List[Measurement]) {

  override def toString = s"Metric($name,$mtype)"

  def asMetric = Metric(name, mtype)

  def asHistogramBuckets = measurements.map(measurement ⇒ new HistogramBucket(measurement.ts, 1 millis, histogramOf(measurement.values)))

  def asCounterBuckets = ???

  private def histogramOf(values: Seq[Long]): Histogram = {
    val histogram = HistogramBucket.newHistogram
    values.foreach(histogram.recordValue(_))
    histogram
  }

}

case class Measurement(ts: Long, values: Seq[Long])

case class MetricBatch(metrics: List[MetricMeasurement])

object MetricBatchProtocol extends DefaultJsonProtocol with SprayJsonSupport with CollectionFormats {
  implicit val MeasurementFormat = jsonFormat2(Measurement)
  implicit val MetricFormat = jsonFormat3(MetricMeasurement)
  implicit val MetricBatchFormat = jsonFormat1(MetricBatch)
  //  implicit object MetricMeasurementJsonFormat extends RootJsonFormat[MetricMeasurement] {
  //    def write(m: MetricMeasurement) = 
  //      JsObject(("name",JsString(m.name)), ("mtype",JsString(m.mtype)),("measurements", listFormat[Measurement].write(m.measurements)))
  //
  //    def read(value: JsValue) = value match {
  //      case JsObject(Map("name" -> JsString(name), "mtype" -> JsString(mtype), "measurements" -> JsArray(JsObject)) =>
  //        new MetricMeasurement(name, mtype, green.toInt)
  //      case _ => deserializationError("MetricMeasurement expected")
  //    }
  //  }
}