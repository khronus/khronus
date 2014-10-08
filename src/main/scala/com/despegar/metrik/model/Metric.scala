package com.despegar.metrik.model

import spray.json.DefaultJsonProtocol
import spray.httpx.SprayJsonSupport
import spray.json.CollectionFormats
import scala.concurrent.duration._
import org.HdrHistogram.Histogram

case class Metric(name: String, mtype: String, measurements: List[Measurement]) {

  override def toString = s"Metric($name,$mtype)"
  
  def asHistogramBuckets = measurements.map(measurement => HistogramBucket(measurement.ts, 1 millis, histogramOf(measurement.values)))

  private def histogramOf(values: Seq[Long]): Histogram = {
    val histogram = HistogramBucket.newHistogram
    values.foreach(histogram.recordValue(_))
    histogram
  }

}

case class Measurement(ts: Long, values: Seq[Long])

case class MetricBatch(metrics: List[Metric])

object MetricBatchProtocol extends DefaultJsonProtocol with SprayJsonSupport with CollectionFormats {
  implicit val MeasurementFormat = jsonFormat2(Measurement)
  implicit val MetricFormat = jsonFormat3(Metric)
  implicit val MetricBatchFormat = jsonFormat1(MetricBatch)
}