package com.despegar.metrik.web.service.influx

import spray.json.DefaultJsonProtocol
import spray.json._
import spray.httpx.SprayJsonSupport

/**
 * Transfer object which is returned by Influx queries
 */
case class InfluxSeries(name: String, columns: Vector[String] = Vector.empty, points: Vector[Vector[Long]] = Vector.empty)

object InfluxSeriesProtocol extends DefaultJsonProtocol with SprayJsonSupport with CollectionFormats {
  implicit val influxSeriesFormat = jsonFormat3(InfluxSeries)
}