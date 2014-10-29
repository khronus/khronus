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
import scala.concurrent.{ Future, Promise }

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

case class Slice(from: Long, to: Long)

trait InfluxQueryResolver extends MetaSupport with StatisticSummarySupport {
  this: InfluxService ⇒

  lazy val parser = new InfluxQueryParser

  def search(query: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Starting Influx query [$query]")

    if (ListSeries.equalsIgnoreCase(query)) metaStore.retrieveMetrics.map(results ⇒ results.map(x ⇒ new InfluxSeries(x)))
    else {

      def toInfluxSeries(summaries: Seq[StatisticSummary], projection: Projection, key: String): InfluxSeries = projection match {
        case Field(name, _) ⇒ {
          log.info(s"Building Influx series: Key $key - Projection: $projection")
          val points = summaries.foldLeft(Vector.empty[Vector[Long]]) {
            (acc, current) ⇒
              acc :+ Vector(current.timestamp, current.get(name)) //TODO: should avoid reflection strategy?
          }
          InfluxSeries(key, Vector("time", name), points)
        }
        case everythingElse ⇒ InfluxSeries("none")
      }

      def buildSlice(filters: List[Filter]): Slice = {
        var from = -1L
        var to = System.currentTimeMillis()
        filters foreach {
          case filter: IntervalFilter ⇒ {
            filter.operator match {
              case Operators.Gt  ⇒ from = filter.value + 1
              case Operators.Gte ⇒ from = filter.value
              case Operators.Lt  ⇒ to = filter.value - 1
              case Operators.Lte ⇒ to = filter.value
            }
          }
          case StringFilter(_, _, _) ⇒ //TODO
        }
        Slice(from, to)
      }

      parser.parse(query) flatMap { influxCriteria ⇒
        for {
          key ← Some(influxCriteria.table)
          projection ← Some(influxCriteria.projection)
          groupBy ← Some(influxCriteria.groupBy)
          filters ← influxCriteria.filters
          limit ← influxCriteria.limit
        } yield {
          val slice = buildSlice(filters)
          statisticSummaryStore.readAll(groupBy.duration, key.name, slice.from, slice.to, limit) map {
            results ⇒ Seq(toInfluxSeries(results, projection, key.name))
          }
        }
      }
    }.getOrElse(Promise[Seq[InfluxSeries]].future)
  }
}