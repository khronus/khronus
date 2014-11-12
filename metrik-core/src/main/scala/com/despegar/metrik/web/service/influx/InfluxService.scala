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

package com.despegar.metrik.web.service.influx

import akka.actor.Props
import com.despegar.metrik.util.Logging
import spray.http.MediaTypes._
import spray.http.StatusCodes._
import spray.httpx.encoding.{ NoEncoding, Gzip }
import spray.routing.{ Route, HttpServiceActor, HttpService }
import InfluxSeriesProtocol.influxSeriesFormat

import scala.concurrent.ExecutionContext.Implicits.global

class InfluxActor extends HttpServiceActor with InfluxEndpoint {
  def receive = runRoute(influxServiceRoute)
}

object InfluxActor {
  def props = Props[InfluxActor]
}

trait InfluxEndpoint extends HttpService with Logging with CORSSupport with InfluxQueryResolver with DashboardSupport {
  import DashboardProtocol._

  val influxServiceRoute: Route =
    compressResponse(NoEncoding, Gzip) {
      respondWithCORS {
        path("series") {
          get {
            parameters('q.?, 'p, 'u) { (query, password, username) ⇒
              query.map { q ⇒
                log.info(s"GET /metrik/influx - Query: [$q]")
                respondWithMediaType(`application/json`) { complete { search(q) } }
              } getOrElse { complete { (OK, s"Authenticated with username: $username and password: " + password) } }
            }
          }
        } ~
          path("dashboards" / "series") {
            get {
              parameters('q) { query ⇒
                respondWithMediaType(`application/json`) {
                  complete { (OK, dashboardResolver.lookup(query)) }
                }
              }
            } ~
              post {
                entity(as[Seq[Dashboard]]) { dashboards ⇒
                  complete { dashboardResolver.store(dashboards.head) }
                }
              }
          }
      }
    }
}

