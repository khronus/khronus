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

import com.despegar.khronus.influx.parser._
import com.despegar.khronus.influx.service.{ InfluxEndpoint, InfluxSeries }
import com.despegar.khronus.model._
import com.despegar.khronus.store.{ Summaries, SummaryStore, Slice, MetaSupport }
import com.despegar.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.SeqView

trait InfluxQueryResolver extends MetaSupport with Measurable with ConcurrencySupport {
  this: InfluxEndpoint ⇒

  import com.despegar.khronus.influx.finder.InfluxQueryResolver._

  implicit val executionContext: ExecutionContext = executionContext("influx-query-resolver-worker")
  val parser = new InfluxQueryParser

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

    parser.parse(expression).map {
      influxCriteria ⇒

        val slice = buildSlice(influxCriteria.filters)
        val timeWindow = adjustResolution(slice, influxCriteria.groupBy)
        val timeRangeMillis = buildTimeRangeMillis(slice, timeWindow)

        val summariesBySourceMap = getSummariesBySourceMap(influxCriteria, timeWindow, slice)
        buildInfluxSeries(influxCriteria.projections, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)

    }.flatMap(Future.sequence(_))
  }

  private def buildSlice(filters: Seq[Filter]): Slice = {
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
      throw new UnsupportedOperationException("From clause required");

    Slice(from, to)
  }

  protected def now = System.currentTimeMillis()

  private def adjustResolution(slice: Slice, groupBy: GroupBy): FiniteDuration = {
    val sortedWindows = Settings.Window.ConfiguredWindows.toSeq.sortBy(_.toMillis).reverse
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

  private def resolution(slice: Slice, timeWindow: FiniteDuration) = {
    Math.abs(slice.to - slice.from) / timeWindow.toMillis
  }

  private def millisBetween(some: FiniteDuration, other: FiniteDuration) = Math.abs(some.toMillis - other.toMillis)

  private def buildTimeRangeMillis(slice: Slice, timeWindow: FiniteDuration): TimeRangeMillis = {
    val alignedFrom = alignTimestamp(slice.from, timeWindow, floorRounding = false)
    val alignedTo = alignTimestamp(slice.to, timeWindow, floorRounding = true)
    TimeRangeMillis(alignedFrom, alignedTo, timeWindow.toMillis)
  }

  private def alignTimestamp(timestamp: Long, timeWindow: FiniteDuration, floorRounding: Boolean): Long = {
    if (timestamp % timeWindow.toMillis == 0)
      timestamp
    else {
      val division = timestamp / timeWindow.toMillis
      if (floorRounding) division * timeWindow.toMillis else (division + 1) * timeWindow.toMillis
    }
  }

  private def getSummariesBySourceMap(influxCriteria: InfluxCriteria, timeWindow: FiniteDuration, slice: Slice) = {
    influxCriteria.sources.foldLeft(Map.empty[String, Future[Map[Long, Summary]]])((acc, source) ⇒ {
      val tableId = source.alias.getOrElse(source.metric.name)
      val summaries = getStore(source.metric.mtype).readAll(source.metric.name, timeWindow, slice, influxCriteria.orderAsc, influxCriteria.limit)
      val summariesByTs = summaries.map(f ⇒ f.foldLeft(Map.empty[Long, Summary])((acc, summary) ⇒ acc + (summary.timestamp.ms -> summary)))
      acc + (tableId -> summariesByTs)
    })
  }

  private def getStore(metricType: String) = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ getStatisticSummaryStore
    case MetricType.Counter                  ⇒ getCounterSummaryStore
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }

  protected def getStatisticSummaryStore: SummaryStore[StatisticSummary] = Summaries.histogramSummaryStore

  protected def getCounterSummaryStore: SummaryStore[CounterSummary] = Summaries.counterSummaryStore

  private def buildInfluxSeries(projections: Seq[SimpleProjection], timeRangeMillis: TimeRangeMillis, summariesBySourceMap: Map[String, Future[Map[Long, Summary]]], defaultValue: Option[Long]): Seq[Future[InfluxSeries]] = {
    projections.map {
      case field: Field ⇒ {
        generateSeq(field, timeRangeMillis, summariesBySourceMap, defaultValue).map(values ⇒
          toInfluxSeries(values, field.alias.getOrElse(field.name), field.tableId.get))
      }
      case number: Number ⇒ {
        generateSeq(number, timeRangeMillis, summariesBySourceMap, defaultValue).map(values ⇒
          toInfluxSeries(values, number.alias.get))
      }
      case operation: Operation ⇒ {
        for {
          leftValues ← generateSeq(operation.left, timeRangeMillis, summariesBySourceMap, defaultValue)
          rightValues ← generateSeq(operation.right, timeRangeMillis, summariesBySourceMap, defaultValue)
        } yield {
          val resultedValues = zipByTimestamp(leftValues, rightValues, operation.operator)
          toInfluxSeries(resultedValues, operation.alias)
        }
      }
    }
  }

  private def generateSeq(simpleProjection: SimpleProjection, timeRangeMillis: TimeRangeMillis, summariesMap: Map[String, Future[Map[Long, Summary]]], defaultValue: Option[Long]): Future[Map[Long, Long]] =
    simpleProjection match {
      case field: Field   ⇒ generateSummarySeq(timeRangeMillis, field.name, summariesMap(field.tableId.get), defaultValue)
      case number: Number ⇒ generateScalarSeq(timeRangeMillis, number.value)
      case _              ⇒ throw new UnsupportedOperationException("Nested operations are not supported yet")
    }

  private def generateScalarSeq(timeRangeMillis: TimeRangeMillis, scalar: Double): Future[Map[Long, Long]] = {
    val roundedScalar = math.round(scalar)
    Future { (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).map(ts ⇒ ts -> roundedScalar).toMap }
  }

  private def generateSummarySeq(timeRangeMillis: TimeRangeMillis, function: String, summariesByTs: Future[Map[Long, Summary]], defaultValue: Option[Long]): Future[Map[Long, Long]] = {
    summariesByTs.map(summariesMap ⇒ {
      (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).foldLeft(Map.empty[Long, Long])((acc, currentTimestamp) ⇒
        if (summariesMap.get(currentTimestamp).isDefined) {
          acc + (currentTimestamp -> summariesMap(currentTimestamp).get(function))
        } else if (defaultValue.isDefined) {
          acc + (currentTimestamp -> defaultValue.get)
        } else {
          acc
        })
    })
  }

  private def zipByTimestamp(tsValues1: Map[Long, Long], tsValues2: Map[Long, Long], operator: MathOperators.MathOperator): Map[Long, Long] = {
    val zippedByTimestamp = for (timestamp ← tsValues1.keySet.intersect(tsValues2.keySet))
      yield (timestamp, calculate(tsValues1(timestamp), tsValues2(timestamp), operator))

    zippedByTimestamp.toMap
  }

  private def calculate(firstOperand: Long, secondOperand: Long, operator: MathOperators.MathOperator): Long = {
    operator(firstOperand, secondOperand)
  }

  private def toInfluxSeries(timeSeriesValues: Map[Long, Long], projectionName: String, metricName: String = ""): InfluxSeries = {
    log.info(s"Building Influx serie for projection [$projectionName] - Metric [$metricName]")

    val points = timeSeriesValues.foldLeft(Vector.empty[Vector[Long]])((acc, current) ⇒ acc :+ Vector(current._1, current._2))
    InfluxSeries(metricName, Vector(influxTimeKey, projectionName), points)
  }

}

case class TimeRangeMillis(from: Long, to: Long, timeWindow: Long)

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"

}