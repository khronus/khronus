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

package com.searchlight.khronus.influx.service

import akka.actor.Props
import com.searchlight.khronus.influx.finder.{ DashboardSupport, InfluxQueryResolver }
import com.searchlight.khronus.service.KhronusHandlerException
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ JacksonJsonSupport, CORSSupport, ConcurrencySupport }
import spray.http.MediaTypes._
import spray.http.StatusCodes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing.{ HttpService, HttpServiceActor, Route }

import scala.concurrent.{ Await, ExecutionContext }

class InfluxActor extends HttpServiceActor with InfluxEndpoint with KhronusHandlerException {
  def receive = runRoute(influxServiceRoute)
}

object InfluxActor {
  def props = Props[InfluxActor]

  val Name = "influx-actor"
  val Path = "khronus/db/influx"
}

trait InfluxEndpoint extends HttpService with JacksonJsonSupport with Logging with CORSSupport with InfluxQueryResolver with DashboardSupport with ConcurrencySupport {

  implicit val ex: ExecutionContext = executionContext("influx-endpoint-worker")

  val influxServiceRoute: Route =
    compressResponse(NoEncoding, Gzip) {
      respondWithCORS {
        path("series") {
          get {
            parameters('q.?, 'p, 'u) { (query, password, username) ⇒
              query.map { q ⇒
                log.info(s"GET /khronus/influx - Query: [$q]")
                respondWithMediaType(`application/json`) {
                  complete {
                    search(q)
                  }
                }
              } getOrElse {
                complete {
                  (OK, s"Authenticated with username: $username and password: " + password)
                }
              }
            }
          }
        } ~
          path("dashboards" / "series") {
            get {
              parameters('q) { query ⇒
                respondWithMediaType(`application/json`) {
                  complete {
                    dashboardResolver.dashboardOperation(query)
                  }
                }
              }
            } ~
              post {
                entity(as[Seq[Dashboard]]) { dashboards ⇒
                  complete {
                    dashboardResolver.store(dashboards.head)
                  }
                }
              }
          }
      }
    }
}

