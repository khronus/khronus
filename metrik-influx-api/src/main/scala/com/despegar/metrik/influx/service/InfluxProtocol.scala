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

package com.despegar.metrik.influx.service

import spray.httpx.SprayJsonSupport
import spray.json._

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