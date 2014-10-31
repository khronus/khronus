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

import akka.actor.Actor
import com.despegar.metrik.util.Logging
import spray.http.MediaTypes._
import spray.http.StatusCodes._
import spray.httpx.encoding.{ NoEncoding, Gzip }
import spray.routing.HttpService

import scala.concurrent.ExecutionContext.Implicits.global

class InfluxActor extends Actor with InfluxService {
  def actorRefFactory = context

  def receive = runRoute(influxRoute)
}

trait InfluxService extends HttpService with Logging with CORSSupport with InfluxQueryResolver {

  import InfluxSeriesProtocol._

  val influxRoute =
    compressResponse(NoEncoding, Gzip) {
      respondWithCORS {
        path("metrik" / "influx" / "series") {
          get {
            parameters('q.?, 'p, 'u) { (query, password, username) ⇒
              query.map { q ⇒
                log.info(s"GET /metrik/influx - Query: [$q]")
                respondWithMediaType(`application/json`) { complete { search(q) } }
              } getOrElse { complete { (OK, s"Authenticated with username: $username and password: " + password) } }
            }
          }
        }
      }
    }
}

