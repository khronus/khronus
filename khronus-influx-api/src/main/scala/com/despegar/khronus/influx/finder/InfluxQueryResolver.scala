/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.influx.finder

import com.despegar.khronus.influx.parser.{ Field, StringFilter, TimeFilter, _ }
import com.despegar.khronus.influx.service.{ InfluxEndpoint, InfluxSeries }
import com.despegar.khronus.model.{ CounterSummary, StatisticSummary, _ }
import com.despegar.khronus.store.{ Slice, _ }
import com.despegar.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

trait InfluxQueryResolver extends MetaSupport with Measurable with ConcurrencySupport {
  this: InfluxEndpoint ⇒

  import com.despegar.khronus.influx.finder.InfluxQueryResolver._

  implicit val executionContext: ExecutionContext = executionContext("influx-query-resolver-worker")
  lazy val parser = new InfluxQueryParser

  def search(search: String): Future[Seq[InfluxSeries]] = search match {
    case GetSeriesPattern(expression) ⇒ listSeries(s".*$expression.*")
    case query                        ⇒ executeQuery(query)
  }

  private def listSeries(expression: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Listing series $expression")
    metaStore.searchInSnapshot(expression).map(results ⇒ results.map(x ⇒ new InfluxSeries(x.name)))
  }

  private def executeQuery(expression: String): Future[Seq[InfluxSeries]] = measureFutureTime("executeInfluxQuery", "executeInfluxQuery") {
    log.info(s"Executing query [$expression]")

    parser.parse(expression).map(influxCriteria ⇒ {
      Future.sequence(influxCriteria.tables.collect {
        case metric ⇒ getInfluxSeries(metric, influxCriteria)
      }).map(series ⇒ series.flatten)
    }).flatMap(x ⇒ x)
  }

  private def getInfluxSeries(metric: Metric, influxCriteria: InfluxCriteria): Future[Seq[InfluxSeries]] = {
    val slice = buildSlice(influxCriteria.filters)
    val timeWindow = adjustResolution(slice, influxCriteria.groupBy, metric.mtype)

    getStore(metric.mtype).readAll(metric.name, timeWindow, slice, influxCriteria.orderAsc, influxCriteria.limit).map {
      summaries ⇒ toInfluxSeries(summaries, influxCriteria.projections, metric.name)
    }
  }

  private def adjustResolution(slice: Slice, groupBy: GroupBy, metricType: String): FiniteDuration = {
    val sortedWindows = getConfiguredWindows(metricType).sortBy(_.toMillis).reverse
    val desiredTimeWindow = groupBy.duration
    val nearestConfiguredWindow = sortedWindows.foldLeft(sortedWindows.last)((nearest, next) ⇒ if (millisBetween(desiredTimeWindow, next) < millisBetween(desiredTimeWindow, nearest)) next else nearest)

    if (groupBy.forceResolution) {
      nearestConfiguredWindow
    } else {
      val points = resolution(slice, nearestConfiguredWindow)
      if (points <= maxResolution & points >= minResolution)
        nearestConfiguredWindow
      else {
        sortedWindows.foldLeft(sortedWindows.head)((adjustedWindow, next) ⇒ {
          val points = resolution(slice, next)
          if (points >= minResolution & points <= maxResolution)
            next
          else if (points < minResolution) next else adjustedWindow
        })
      }
    }
  }
  protected lazy val maxResolution: Int = Settings.Dashboard.MaxResolutionPoints
  protected lazy val minResolution: Int = Settings.Dashboard.MinResolutionPoints

  protected def getConfiguredWindows(metricType: String): Seq[FiniteDuration] = Settings.getConfiguredWindows(metricType)

  private def resolution(slice: Slice, timeWindow: FiniteDuration) = {
    Math.abs(slice.to - slice.from) / timeWindow.toMillis
  }

  private def millisBetween(some: FiniteDuration, other: FiniteDuration) = Math.abs(some.toMillis - other.toMillis)

  private def getStore(metricType: String) = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ getStatisticSummaryStore
    case MetricType.Counter                  ⇒ getCounterSummaryStore
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }
  protected def getStatisticSummaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
  protected def getCounterSummaryStore: SummaryStore[CounterSummary] = CassandraCounterSummaryStore

  private def toInfluxSeries(summaries: Seq[Summary], functions: Seq[Field], metricName: String): Seq[InfluxSeries] = {
    log.info(s"Building Influx series: Metric $metricName - Functions: $functions - Summaries count: ${summaries.size}")

    val pointsPerFunction = TrieMap[String, Vector[Vector[Long]]]()

    summaries.foreach(summary ⇒ {
      functions.foreach(function ⇒ {
        val id = function.alias.getOrElse(function.name)
        pointsPerFunction.put(id, pointsPerFunction.getOrElse(id, Vector.empty) :+ Vector(summary.timestamp.ms, summary.get(function.name)))
      })
    })

    pointsPerFunction.collect {
      case (functionName, points) ⇒ InfluxSeries(metricName, Vector(influxTimeKey, functionName), points)
    }.toSeq
  }

  private def buildSlice(filters: List[Filter]): Slice = {
    var from = -1L
    var to = now
    filters foreach {
      case filter: TimeFilter ⇒ {
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

  protected def now = System.currentTimeMillis()
}

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"

}