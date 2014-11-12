package com.despegar.metrik.influx

import spray.json._
import spray.httpx.SprayJsonSupport

case class InfluxSeries(name: String, columns: Vector[String] = Vector.empty, points: Vector[Vector[Long]] = Vector.empty)

object InfluxSeriesProtocol extends DefaultJsonProtocol with SprayJsonSupport with CollectionFormats {
  implicit val influxSeriesFormat = jsonFormat3(InfluxSeries)
}

case class Dashboard(name: String, columns: Vector[String] = Vector.empty, points: Vector[Vector[String]] = Vector.empty)

object DashboardProtocol extends DefaultJsonProtocol with SprayJsonSupport with CollectionFormats {

  implicit object DashboardWriter extends RootJsonFormat[Dashboard] {

    def write(obj: Dashboard): JsValue = {

      //Ugly but grafana sends us JSON with heterogeneous elements and we need extract these by position...
      def extract(elements: Vector[String]) = JsArray(
        JsNumber(elements(0)),
        JsNumber(elements(1)),
        elements(2).toJson,
        elements(3).toJson,
        elements(4).toJson,
        elements(5).toJson)

      JsObject(
        "name" -> JsString(obj.name),
        "columns" -> obj.columns.toSeq.toJson,
        "points" -> JsArray(obj.points.toList.collect { case points ⇒ extract(points) }))
    }

    def read(value: JsValue) = {
      value.asJsObject.getFields("name", "columns", "points") match {
        case Seq(JsString(name), JsArray(columns), JsArray(points)) ⇒
          val currentPoints: List[Vector[String]] = points.map {
            case JsArray(elements) ⇒ elements.map {
              case JsString(str) ⇒ str
              case number        ⇒ number.toString()
            }.toVector
            case anythingElse ⇒ Vector.empty[String]
          }
          new Dashboard(name, columns.map(_.convertTo[String]).toVector, currentPoints.toVector)
        case anythingElse ⇒ throw new DeserializationException("DashBoard expected")
      }
    }
  }
}