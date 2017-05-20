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

package com.searchlight.khronus.influx.finder

import com.searchlight.khronus.dao.{ MetricMetadata, MetaSupport }
import com.searchlight.khronus.influx.parser._
import com.searchlight.khronus.influx.service.{ InfluxEndpoint, InfluxSeries }
import com.searchlight.khronus.model
import com.searchlight.khronus.model.Functions._
import com.searchlight.khronus.model._
import com.searchlight.khronus.model.query._
import com.searchlight.khronus.service.QueryService
import com.searchlight.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

trait InfluxQueryResolver extends MetaSupport with Measurable with ConcurrencySupport {
  this: InfluxEndpoint ⇒

  import com.searchlight.khronus.influx.finder.InfluxQueryResolver._

  implicit val executionContext: ExecutionContext = executionContext("influx-query-resolver-worker")
  val parser = new InfluxQueryParser

  protected def queryService = InfluxQueryResolver.queryService

  def search(search: String): Future[Seq[InfluxSeries]] = search match {
    case GetSeriesPattern(expression) ⇒ listSeries(s".*$expression.*")
    case query ⇒ executeQuery(query).andThen {
      case Failure(reason) ⇒ log.error("search error", reason)
    }
  }

  private def listSeries(expression: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Listing series $expression")
    val points = metaStore.searchMetrics(expression).
      foldLeft(Vector.empty[Vector[Any]])((acc, current) ⇒ acc :+ Vector(0, current.name))

    Future.successful(Seq(InfluxSeries("list_series_result", Vector("time", "name"), points)))
  }

  private def executeQuery(expression: String): Future[Seq[InfluxSeries]] = measureFutureTime("executeInfluxQuery", "executeInfluxQuery") {
    log.info(s"Executing query [$expression]")

    parser.parse(expression).flatMap { criteria ⇒
      val slice = buildSlice(criteria.filters)
      val resolution = adjustResolution(slice, criteria.groupBy)

      val query = createQuery(criteria, slice, resolution)

      queryService.executeQuery(query).map { seriesSeq ⇒
        seriesSeq.map { series ⇒
          InfluxSeries(series.name, Vector("time", "value"), series.points.map(point ⇒ Vector(point.timestamp, point.value)).toVector)
        }
      }
    }
  }

  private def createInfluxProjection(metric: MetricMetadata)(projection: SimpleProjection): model.query.Projection = {
    projection match {
      case field: Field ⇒ Functions.withName(field.name) match {
        //case Count ⇒ service.Count(Selector(metric.name))
        case Max  ⇒ ???
        case Mean ⇒ ???
        case Min  ⇒ ???
        //case p: Percentile ⇒ service.Percentile(Selector(metric.name), p.value)
      }
      case _ ⇒ ???
    }
  }

  private def createQuery(influxCriteria: InfluxCriteria, slice: TimeRange, resolution: Duration): Query = {
    val metric = influxCriteria.sources.head.metric
    val alias = influxCriteria.sources.head.alias.getOrElse(metric.name)
    val predicate = And(influxCriteria.filters.collect { case sf: StringFilter ⇒ Equals(Selector(metric.name), sf.identifier, sf.value) })
    Query(projections = influxCriteria.projections.map(createInfluxProjection(metric)),
      selectors = Seq(Selector(metric.name, Some(alias))), Some(slice), filter = Some(predicate), Some(resolution))
  }

  private def buildSlice(filters: Seq[InfluxFilter]): TimeRange = {
    var from = 1L
    var to = now
    filters foreach {
      case filter: TimeFilter ⇒
        filter.operator match {
          case Operators.Gt  ⇒ from = filter.value + 1
          case Operators.Gte ⇒ from = filter.value
          case Operators.Lt  ⇒ to = filter.value - 1
          case Operators.Lte ⇒ to = filter.value
        }
      case StringFilter(_, _, _) ⇒ //TODO
    }

    if (from == 1L)
      throw new UnsupportedOperationException("From clause required")

    TimeRange(from, to)
  }

  protected def now = System.currentTimeMillis()

  private def adjustResolution(slice: TimeRange, groupBy: GroupBy): Duration = {
    val desiredTimeWindow = groupBy.duration
    val forceResolution = groupBy.forceResolution
    slice.getAdjustedResolution(desiredTimeWindow, forceResolution, minResolution, maxResolution)
  }

  protected lazy val maxResolution: Int = Settings.Dashboard.MaxResolutionPoints
  protected lazy val minResolution: Int = Settings.Dashboard.MinResolutionPoints

}

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"
  private val queryService = QueryService()

}

case class TimeRangeMillis(from: Long, to: Long, timeWindow: Long)
