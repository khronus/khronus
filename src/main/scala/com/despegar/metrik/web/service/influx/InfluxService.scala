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

import spray.routing.HttpService
import akka.actor.Actor
import spray.http.MediaTypes._
import com.despegar.metrik.store.MetaSupport
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.despegar.metrik.util.Logging

/**
 * Created by aholman on 21/10/14.
 */
class InfluxActor extends Actor with InfluxService {

  def actorRefFactory = context

  def receive = runRoute(influxRoute)

}

case class QueryString(queryString: String)

trait InfluxService extends HttpService with MetaSupport with Logging with CORSSupport {
  val ListSeries = "list series"

  val influxRoute =
    respondWithCORS {
      path("metrik" / "influx" / "series") {

        parameters('q) {
          queryString ⇒
            get {
              log.info(s"GET /metrik/influx - Query: [$queryString]")
              respondWithMediaType(`application/json`) {
                complete {
                  import com.despegar.metrik.web.service.influx.InfluxSeriesProtocol._
                  resolveInfluxQuery(queryString)
                }
              }
            }
        }
      }
    }

  def resolveInfluxQuery(queryString: String): Future[Seq[InfluxSeries]] = {
    if (ListSeries.equalsIgnoreCase(queryString)) {
      log.info("Starting Influx list series")
      metaStore.retrieveMetrics map {
        results ⇒
          results.map {
            x ⇒ new InfluxSeries(x)
          }
      }
    } else {
      log.error(s"Influx query [$queryString] is not supported")
      throw new UnsupportedOperationException(s"Influx query [$queryString] is not supported")
    }

  }
}

