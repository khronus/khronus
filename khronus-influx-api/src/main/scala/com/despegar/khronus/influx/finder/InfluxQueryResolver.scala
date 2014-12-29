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
        val alignedFrom = alignTimestamp(slice.from, timeWindow, false)
        val alignedTo = alignTimestamp(slice.to, timeWindow, true)

        val summariesBySourceMap = influxCriteria.sources.foldLeft(Map.empty[String, Future[Map[Long, Summary]]])((acc, source) ⇒ {
          val tableId = source.alias.getOrElse(source.metric.name)
          val summaries = getStore(source.metric.mtype).readAll(source.metric.name, timeWindow, slice, influxCriteria.orderAsc, influxCriteria.limit)
          val summariesByTs = summaries.map(f ⇒ f.foldLeft(Map.empty[Long, Summary])((acc, summary) ⇒ acc + (summary.timestamp.ms -> summary)))
          acc + (tableId -> summariesByTs)
        })

        val futuresInfluxSeries = influxCriteria.projections.map {
          case field: Field ⇒ {
            generateSeq(field, alignedFrom, alignedTo, timeWindow, summariesBySourceMap).map(values ⇒
              toInfluxSeries(values, field.alias.getOrElse(field.name), field.tableId.get))
          }
          case number: Number ⇒ {
            generateSeq(number, alignedFrom, alignedTo, timeWindow, summariesBySourceMap).map(values ⇒
              toInfluxSeries(values, number.alias.get))
          }
          case operation: Operation ⇒ {
            for {
              leftValues ← generateSeq(operation.left, alignedFrom, alignedTo, timeWindow, summariesBySourceMap)
              rightValues ← generateSeq(operation.right, alignedFrom, alignedTo, timeWindow, summariesBySourceMap)
            } yield {
              val resultedValues = leftValues.zip(rightValues).foldLeft(Seq.empty[TimeSeriesValue])((acc, tuple) ⇒ acc :+ calculate(tuple._1, tuple._2, operation.operator))
              toInfluxSeries(resultedValues.view, operation.alias)
            }
          }
        }

        futuresInfluxSeries
    }.flatMap(Future.sequence(_).map(_.flatten))
  }

  case class TimeSeriesValue(timestamp: Long, value: Long)

  private def alignTimestamp(timestamp: Long, timeWindow: FiniteDuration, floorRounding: Boolean): Long = {
    if (timestamp % timeWindow.toMillis == 0)
      timestamp
    else {
      val division = timestamp / timeWindow.toMillis
      if (floorRounding) division * timeWindow.toMillis else (division + 1) * timeWindow.toMillis
    }
  }

  private def calculate(firstOperand: TimeSeriesValue, secondOperand: TimeSeriesValue, operator: MathOperators.MathOperator): TimeSeriesValue = {
    val result = operator match {
      case MathOperators.Plus     ⇒ firstOperand.value + secondOperand.value
      case MathOperators.Minus    ⇒ firstOperand.value - secondOperand.value
      case MathOperators.Multiply ⇒ firstOperand.value * secondOperand.value
      case MathOperators.Divide   ⇒ firstOperand.value / secondOperand.value
    }
    TimeSeriesValue(firstOperand.timestamp, result)
  }

  private def generateSeq(simpleProjection: SimpleProjection, alignedFrom: Long, alignedTo: Long, timeWindow: FiniteDuration, summariesMap: Map[String, Future[Map[Long, Summary]]]): Future[SeqView[TimeSeriesValue, Seq[_]]] =
    simpleProjection match {
      case field: Field   ⇒ generateSummarySeq(alignedFrom, alignedTo, timeWindow.toMillis, field.name, summariesMap(field.tableId.get))
      case number: Number ⇒ generateScalarSeq(alignedFrom, alignedTo, timeWindow.toMillis, number.value)
      case _              ⇒ throw new UnsupportedOperationException("Nested operations are not supported yet")
    }

  private def generateScalarSeq(fromMillis: Long, toMillis: Long, timeWindow: Long, scalar: Double): Future[SeqView[TimeSeriesValue, Seq[_]]] = {
    Future { (fromMillis to toMillis by timeWindow).view.map(ts ⇒ TimeSeriesValue(ts, math.round(scalar))) }
  }

  private def generateSummarySeq(fromMillis: Long, toMillis: Long, timeWindow: Long, function: String, summariesByTs: Future[Map[Long, Summary]]): Future[SeqView[TimeSeriesValue, Seq[_]]] = {
    summariesByTs.map(summariesMap ⇒ {
      (fromMillis to toMillis by timeWindow).view.collect {
        case timestamp ⇒
          val value = summariesMap.get(timestamp) match {
            case Some(summary) ⇒ summary.get(function)
            case None          ⇒ 0
          }
          TimeSeriesValue(timestamp, value)
      }
    })
  }

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

  private def getStore(metricType: String) = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ getStatisticSummaryStore
    case MetricType.Counter                  ⇒ getCounterSummaryStore
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }

  protected def getStatisticSummaryStore: SummaryStore[StatisticSummary] = Summaries.histogramSummaryStore

  protected def getCounterSummaryStore: SummaryStore[CounterSummary] = Summaries.counterSummaryStore

  private def toInfluxSeries(timeSeriesValues: SeqView[TimeSeriesValue, Seq[_]], projectionName: String, metricName: String = ""): Seq[InfluxSeries] = {
    log.info(s"Building Influx series: Projection [$projectionName] - Metric [$metricName]")

    val pointsPerFunction = TrieMap[String, Vector[Vector[Long]]]()

    timeSeriesValues.foreach(tsValue ⇒ pointsPerFunction.put(projectionName, pointsPerFunction.getOrElse(projectionName, Vector.empty) :+ Vector(tsValue.timestamp, tsValue.value)))

    pointsPerFunction.collect {
      case (functionName, points) ⇒ InfluxSeries(metricName, Vector(influxTimeKey, functionName), points)
    }.toSeq
  }

  private def buildSlice(filters: Seq[Filter]): Slice = {
    var from = 1L
    var to = now
    filters foreach {
      case filter: TimeFilter ⇒ {
        filter.operator match {
          case Operators.Gt  ⇒ { from = filter.value + 1}
          case Operators.Gte ⇒ { from = filter.value}
          case Operators.Lt  ⇒ { to = filter.value - 1}
          case Operators.Lte ⇒ { to = filter.value}
        }
      }
      case StringFilter(_, _, _) ⇒ //TODO
    }

    if (from == 1L)
      throw new UnsupportedOperationException("From clause required");

    Slice(from, to)
  }

  protected def now = System.currentTimeMillis()
}

object InfluxQueryResolver {
  //matches list series /expression/
  val GetSeriesPattern = "list series /(.*)/".r
  val influxTimeKey = "time"

}