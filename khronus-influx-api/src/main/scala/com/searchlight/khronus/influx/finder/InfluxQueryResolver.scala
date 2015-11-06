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

import com.searchlight.khronus.influx.parser._
import com.searchlight.khronus.influx.service.{ InfluxEndpoint, InfluxSeries }
import com.searchlight.khronus.model._
import com.searchlight.khronus.store.{ Summaries, SummaryStore, Slice, MetaSupport }
import com.searchlight.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.SeqView

trait InfluxQueryResolver extends MetaSupport with Measurable with ConcurrencySupport {
  this: InfluxEndpoint ⇒

  import com.searchlight.khronus.influx.finder.InfluxQueryResolver._

  implicit val executionContext: ExecutionContext = executionContext("influx-query-resolver-worker")
  val parser = new InfluxQueryParser

  def search(search: String): Future[Seq[InfluxSeries]] = search match {
    case GetSeriesPattern(expression) ⇒ listSeries(s".*$expression.*")
    case query                        ⇒ executeQuery(query)
  }

  private def listSeries(expression: String): Future[Seq[InfluxSeries]] = {
    log.info(s"Listing series $expression")
    val points = metaStore.searchInSnapshotByRegex(expression).
      foldLeft(Vector.empty[Vector[Any]])((acc, current) ⇒ acc :+ Vector(0, current.name))

    Future.successful(Seq(new InfluxSeries("list_series_result", Vector("time", "name"), points)))
  }

  private def executeQuery(expression: String): Future[Seq[InfluxSeries]] = measureFutureTime("executeInfluxQuery", "executeInfluxQuery") {
    log.info(s"Executing query [$expression]")

    parser.parse(expression).map {
      influxCriteria ⇒

        val slice = buildSlice(influxCriteria.filters)
        val timeWindow = adjustResolution(slice, influxCriteria.groupBy)
        val timeRangeMillis = buildTimeRangeMillis(slice, timeWindow)

        val summariesBySourceMap = getSummariesBySourceMap(influxCriteria, timeWindow, slice)
        buildInfluxSeries(influxCriteria, timeRangeMillis, summariesBySourceMap)

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

  protected def getStatisticSummaryStore: SummaryStore[HistogramSummary] = Summaries.histogramSummaryStore

  protected def getCounterSummaryStore: SummaryStore[CounterSummary] = Summaries.counterSummaryStore

  private def buildInfluxSeries(influxCriteria: InfluxCriteria, timeRangeMillis: TimeRangeMillis, summariesBySourceMap: Map[String, Future[Map[Long, Summary]]]): Seq[Future[InfluxSeries]] = {
    influxCriteria.projections.sortBy(_.seriesId).map {
      case field: Field ⇒ {
        generateSeq(field, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
          toInfluxSeries(values, field.alias.getOrElse(field.name), influxCriteria.orderAsc, influxCriteria.scale, field.tableId.get))
      }
      case number: Number ⇒ {
        generateSeq(number, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
          toInfluxSeries(values, number.alias.get, influxCriteria.orderAsc, influxCriteria.scale))
      }
      case operation: Operation ⇒ {
        for {
          leftValues ← generateSeq(operation.left, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
          rightValues ← generateSeq(operation.right, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
        } yield {
          val resultedValues = zipByTimestamp(leftValues, rightValues, operation.operator)
          toInfluxSeries(resultedValues, operation.alias, influxCriteria.orderAsc, influxCriteria.scale)
        }
      }
    }
  }

  private def generateSeq(simpleProjection: SimpleProjection, timeRangeMillis: TimeRangeMillis, summariesMap: Map[String, Future[Map[Long, Summary]]], defaultValue: Option[Double]): Future[Map[Long, Double]] =
    simpleProjection match {
      case field: Field   ⇒ generateSummarySeq(timeRangeMillis, Functions.withName(field.name), summariesMap(field.tableId.get), defaultValue)
      case number: Number ⇒ generateScalarSeq(timeRangeMillis, number.value)
      case _              ⇒ throw new UnsupportedOperationException("Nested operations are not supported yet")
    }

  private def generateScalarSeq(timeRangeMillis: TimeRangeMillis, scalar: Double): Future[Map[Long, Double]] = {
    Future { (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).map(ts ⇒ ts -> scalar).toMap }
  }

  private def generateSummarySeq(timeRangeMillis: TimeRangeMillis, function: Functions.Function, summariesByTs: Future[Map[Long, Summary]], defaultValue: Option[Double]): Future[Map[Long, Double]] = {
    summariesByTs.map(summariesMap ⇒ {
      (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).foldLeft(Map.empty[Long, Double])((acc, currentTimestamp) ⇒
        if (summariesMap.get(currentTimestamp).isDefined) {
          function match {
            case metaFunction: Functions.MetaFunction ⇒ acc + (currentTimestamp -> metaFunction(summariesMap(currentTimestamp), timeRangeMillis.timeWindow))
            case simpleFunction: Functions.Function   ⇒ acc + (currentTimestamp -> simpleFunction(summariesMap(currentTimestamp)))
          }
        } else if (defaultValue.isDefined) {
          acc + (currentTimestamp -> defaultValue.get)
        } else {
          acc
        })
    })
  }

  private def zipByTimestamp(tsValues1: Map[Long, Double], tsValues2: Map[Long, Double], operator: MathOperators.MathOperator): Map[Long, Double] = {
    val zippedByTimestamp = for (timestamp ← tsValues1.keySet.intersect(tsValues2.keySet))
      yield (timestamp, calculate(tsValues1(timestamp), tsValues2(timestamp), operator))

    zippedByTimestamp.toMap
  }

  private def calculate(firstOperand: Double, secondOperand: Double, operator: MathOperators.MathOperator): Double = {
    operator(firstOperand, secondOperand)
  }

  private def toInfluxSeries(timeSeriesValues: Map[Long, Double], projectionName: String, ascendingOrder: Boolean, scale: Option[Double], metricName: String = ""): InfluxSeries = {
    log.debug(s"Building Influx serie for projection [$projectionName] - Metric [$metricName]")

    val sortedTimeSeriesValues = if (ascendingOrder) timeSeriesValues.toSeq.sortBy(_._1) else timeSeriesValues.toSeq.sortBy(-_._1)

    val points = sortedTimeSeriesValues.foldLeft(Vector.empty[Vector[AnyVal]])((acc, current) ⇒ {
      val value = BigDecimal(current._2 * scale.getOrElse(1d)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
      acc :+ Vector(current._1, value)
    })
    InfluxSeries(metricName, Vector(influxTimeKey, projectionName), points)
  }

}

case class TimeRangeMillis(from: Long, to: Long, timeWindow: Long)

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"

}