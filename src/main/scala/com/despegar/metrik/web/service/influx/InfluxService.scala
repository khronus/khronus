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
import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.store.{ MetaSupport, StatisticSummarySupport }
import com.despegar.metrik.util.Logging
import com.despegar.metrik.web.service.influx.parser._
import spray.http.MediaTypes._
import spray.routing.HttpService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class QueryString(queryString: String)

class InfluxActor extends Actor with InfluxService {
  def actorRefFactory = context
  def receive = runRoute(influxRoute)
}

trait InfluxService extends HttpService with Logging with CORSSupport with InfluxQueryResolver {

  import com.despegar.metrik.web.service.influx.InfluxSeriesProtocol._

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
                  search(queryString)
                }
              }
            }
        }
      }
    }
}

trait InfluxQueryResolver extends MetaSupport with StatisticSummarySupport {
  this: InfluxService ⇒

  lazy val parser = new InfluxQueryParser

  def search(query: String): Future[Seq[InfluxSeries]] = {
    log.info("Starting Influx list series")

    if (ListSeries.equalsIgnoreCase(query)) metaStore.retrieveMetrics.map(results ⇒ results.map(x ⇒ new InfluxSeries(x)))
    else {

      def filterExpression(summary: StatisticSummary, expression: Expression): Boolean = expression match {
        case _ ⇒ true
      }

      //      def toInfluxSeries(summary: StatisticSummary, projection: Projection) = projection match {
      //        // case Field(name, _) => InfluxSeries(name, ); summary.get(name)
      //        case AllField ⇒
      //      }
      null.asInstanceOf[Future[Seq[InfluxSeries]]]
    }
  }

  //      }

  //      val a = parser.parse(query) flatMap { influxCriteria ⇒
  //          for {
  //            key ← Some(influxCriteria.table)
  //            projection ← Some(influxCriteria.projection)
  //            groupBy ← influxCriteria.groupBy
  //            filter ← influxCriteria.filter
  //            limit ← influxCriteria.limit
  //          } yield {
  //             statisticSummaryStore.readAllRows(groupBy.duration, key.name, count = limit) map {
  //              results ⇒
  //                results.filter {
  //                  summary => filterExpression(summary, filter)
  //                }.map(x ⇒ toInfluxSeries(x,projection)))
  //  }
  // }
  //  }
  // a.get
  //}
  //}
}