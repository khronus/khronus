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

package com.despegar.metrik.influx.finder

import com.despegar.metrik.influx.parser._
import com.despegar.metrik.influx.service.{ InfluxSeries, InfluxEndpoint }
import com.despegar.metrik.model._
import com.despegar.metrik.store._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.collection.concurrent.TrieMap
import com.despegar.metrik.model.CounterSummary
import com.despegar.metrik.influx.parser.TimeFilter
import com.despegar.metrik.influx.parser.StringFilter
import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.influx.parser.Field
import com.despegar.metrik.influx.service.InfluxSeries
import com.despegar.metrik.store.Slice
import com.despegar.metrik.util.Settings

trait InfluxQueryResolver extends MetaSupport {
  this: InfluxEndpoint ⇒

  import InfluxQueryResolver._

  lazy val parser = new InfluxQueryParser

  def search(search: String): Future[Seq[InfluxSeries]] = search match {
    case GetSeriesPattern(expression) ⇒ listSeries(s".*$expression.*")
    case query                        ⇒ executeQuery(query)
  }

  private def listSeries(expression: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Listing series $expression")
    metaStore.searchInSnapshot(expression).map(results ⇒ results.map(x ⇒ new InfluxSeries(x.name)))
  }

  private def executeQuery(expression: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Executing query [$expression]")

    val influxCriteria = parser.parse(expression)

    val metricName: String = influxCriteria.table.name
    val metricType = metaStore.getMetricType(metricName)

    val slice = buildSlice(influxCriteria.filters, influxCriteria.orderAsc)
    val timeWindow = adjustResolution(slice, influxCriteria.groupBy.duration, metricType)

    val maxResults: Int = influxCriteria.limit.getOrElse(Int.MaxValue)

    getStore(metricType).readAll(timeWindow, metricName, slice, maxResults).map {
      results ⇒ toInfluxSeries(results, influxCriteria.projections, metricName)
    }
  }

  private def adjustResolution(slice: Slice, desiredTimeWindow: FiniteDuration, metricType: String): FiniteDuration = {
    val sortedWindows = getConfiguredWindows(metricType).sortBy(_.toMillis).reverse
    val nearestConfiguredWindow = sortedWindows.foldLeft(sortedWindows.last)((nearest, next) ⇒ if (millisBetween(desiredTimeWindow, next) < millisBetween(desiredTimeWindow, nearest)) next else nearest)

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

  protected lazy val maxResolution: Int = Settings().Dashboard.MaxResolutionPoints
  protected lazy val minResolution: Int = Settings().Dashboard.MinResolutionPoints
  protected def getConfiguredWindows(metricType: String): Seq[FiniteDuration] = Settings().getConfiguredWindows(metricType)

  private def resolution(slice: Slice, timeWindow: FiniteDuration) = {
    Math.abs(slice.to - slice.from) / timeWindow.toMillis
  }

  private def millisBetween(some: FiniteDuration, other: FiniteDuration) = Math.abs(some.toMillis - other.toMillis)

  private def getStore(metricType: String) = {
    metricType match {
      case MetricType.Timer | MetricType.Gauge   ⇒ getStatisticSummaryStore
      case MetricType.Counter ⇒ getCounterSummaryStore
      case _                  ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
    }
  }

  protected def getStatisticSummaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
  protected def getCounterSummaryStore: SummaryStore[CounterSummary] = CassandraCounterSummaryStore

  private def toInfluxSeries(summaries: Seq[Summary], functions: Seq[Field], metricName: String): Seq[InfluxSeries] = {
    log.info(s"Building Influx series: Metric $metricName - Projections: $functions - Summaries count: ${summaries.size}")

    buildInfluxSeries(summaries, metricName, functions)
  }

  private def buildInfluxSeries(summaries: Seq[Summary], metricName: String, functions: Seq[Field]): Seq[InfluxSeries] = {
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

  private def buildSlice(filters: List[Filter], ascendingOrder: Boolean): Slice = {
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

    if (ascendingOrder)
      Slice(from, to, false)
    else
      Slice(to, from, true)
  }

  protected def now = System.currentTimeMillis()
}

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"

}