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

package com.searchlight.khronus.influx.parser

import com.searchlight.khronus.model._
import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.Some
import scala.concurrent.{ Await, Future }
import com.searchlight.khronus.store.MetaStore
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class InfluxQueryParserSpec extends FunSuite with Matchers with MockitoSugar {
  // TODO - Where con soporte para expresiones regulares: =~ matches against, !~ doesn’t match against

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  val metricName = "metricA"

  def buildParser = new InfluxQueryParser() {
    override val metaStore: MetaStore = mock[MetaStore]
  }

  test("basic Influx query should be parsed ok") {
    val parser = buildParser
    val metricName = """metric:A\12:3"""
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select count(value) from "$metricName" as aliasTable group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Count, None, Some("aliasTable"))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, Some("aliasTable"))

    verifyGroupBy(influxCriteria.groupBy, 2, TimeUnit.HOURS)
    influxCriteria.filters should be(Nil)

    influxCriteria.fillValue should be(None)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select with many projections should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select x.mean, x.max as maxValue, min(value) from "$metricName" as x group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    influxCriteria.projections.size should be(3)

    verifyField(influxCriteria.projections(0), Functions.Mean, None, Some("x"))
    verifyField(influxCriteria.projections(1), Functions.Max, Some("maxValue"), Some("x"))
    verifyField(influxCriteria.projections(2), Functions.Min, None, Some("x"))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, Some("x"))

    verifyGroupBy(influxCriteria.groupBy, 2, TimeUnit.HOURS)
    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select * for a timer should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select aliasTimer.* from "$metricName" as aliasTimer group by time (30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    influxCriteria.projections.size should be(11)
    val sortedProjections = influxCriteria.projections.sortBy(_.asInstanceOf[Field].name)

    verifyField(sortedProjections(0), Functions.Count, None, Some("aliasTimer"))
    verifyField(sortedProjections(1), Functions.Cpm, None, Some("aliasTimer"))
    verifyField(sortedProjections(2), Functions.Max, None, Some("aliasTimer"))
    verifyField(sortedProjections(3), Functions.Mean, None, Some("aliasTimer"))
    verifyField(sortedProjections(4), Functions.Min, None, Some("aliasTimer"))
    verifyField(sortedProjections(5), Functions.Percentile50, None, Some("aliasTimer"))
    verifyField(sortedProjections(6), Functions.Percentile80, None, Some("aliasTimer"))
    verifyField(sortedProjections(7), Functions.Percentile90, None, Some("aliasTimer"))
    verifyField(sortedProjections(8), Functions.Percentile95, None, Some("aliasTimer"))
    verifyField(sortedProjections(9), Functions.Percentile99, None, Some("aliasTimer"))
    verifyField(sortedProjections(10), Functions.Percentile999, None, Some("aliasTimer"))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, Some("aliasTimer"))

    verifyGroupBy(influxCriteria.groupBy, 30, TimeUnit.SECONDS)
    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select * for a counter should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Counter)))

    val query = s"""select * from "$metricName" as aliasCounter group by time (30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    influxCriteria.projections.size should be(2)
    verifyField(influxCriteria.projections(0), Functions.Count, None, Some("aliasCounter"))
    verifyField(influxCriteria.projections(1), Functions.Cpm, None, Some("aliasCounter"))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, Some("aliasCounter"))

    verifyGroupBy(influxCriteria.groupBy, 30, TimeUnit.SECONDS)
    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Select fields for a timer should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryMax = s"""select max, min, mean, count, p50, p80, p90, p95, p99, p999 from "$metricName" group by time(1m)"""

    val projections = await(parser.parse(queryMax)).projections

    verifyField(projections(0), Functions.Max, None, Some(s"$metricName"))
    verifyField(projections(1), Functions.Min, None, Some(s"$metricName"))
    verifyField(projections(2), Functions.Mean, None, Some(s"$metricName"))
    verifyField(projections(3), Functions.Count, None, Some(s"$metricName"))
    verifyField(projections(4), Functions.Percentile50, None, Some(s"$metricName"))
    verifyField(projections(5), Functions.Percentile80, None, Some(s"$metricName"))
    verifyField(projections(6), Functions.Percentile90, None, Some(s"$metricName"))
    verifyField(projections(7), Functions.Percentile95, None, Some(s"$metricName"))
    verifyField(projections(8), Functions.Percentile99, None, Some(s"$metricName"))
    verifyField(projections(9), Functions.Percentile999, None, Some(s"$metricName"))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)
  }

  test("Select fields for a counter should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Counter)))

    val queryCounter = s"""select count(value) from "$metricName" group by time(1m)"""
    val resultedFieldCounter = await(parser.parse(queryCounter)).projections(0)

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(resultedFieldCounter, Functions.Count, None, Some(s"$metricName"))
  }

  test("All Percentiles function should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryAllPercentiles = s"""select percentiles from "$metricName" group by time(30s)"""
    val projections = await(parser.parse(queryAllPercentiles)).projections.sortBy(_.asInstanceOf[Field].name)

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    projections.size should be(6)

    verifyField(projections(0), Functions.Percentile50, None, Some(s"$metricName"))
    verifyField(projections(1), Functions.Percentile80, None, Some(s"$metricName"))
    verifyField(projections(2), Functions.Percentile90, None, Some(s"$metricName"))
    verifyField(projections(3), Functions.Percentile95, None, Some(s"$metricName"))
    verifyField(projections(4), Functions.Percentile99, None, Some(s"$metricName"))
    verifyField(projections(5), Functions.Percentile999, None, Some(s"$metricName"))
  }

  test("Some Percentiles function should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryPercentiles = s"""select percentiles(80 99 50) from "$metricName" group by time(30s)"""
    val projections = await(parser.parse(queryPercentiles)).projections

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    projections.size should be(3)

    verifyField(projections(0), Functions.Percentile80, None, Some(s"$metricName"))
    verifyField(projections(1), Functions.Percentile99, None, Some(s"$metricName"))
    verifyField(projections(2), Functions.Percentile50, None, Some(s"$metricName"))
  }

  test("Counts per minute function should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryPercentiles = s"""select cpm from "$metricName" group by time(5m)"""
    val projections = await(parser.parse(queryPercentiles)).projections

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    projections.size should be(1)

    verifyField(projections(0), Functions.Cpm, None, Some(s"$metricName"))
  }

  test("Projecting operations from single metric should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryMax = s"""select x.p50 + 90 as op1, x.max - x.min as op2, 35 * x.mean as op3, 3 / 4 as op4 from "$metricName" as x group by time(1m)"""

    val projections = await(parser.parse(queryMax)).projections

    val firstOperation = projections(0).asInstanceOf[Operation]
    verifyField(firstOperation.left, Functions.Percentile50, None, Some("x"))
    firstOperation.operator should be(MathOperators.Plus)
    firstOperation.right.asInstanceOf[Number].value should be(90L)
    firstOperation.alias should be("op1")

    val secondOperation = projections(1).asInstanceOf[Operation]
    verifyField(secondOperation.left, Functions.Max, None, Some("x"))
    secondOperation.operator should be(MathOperators.Minus)
    verifyField(secondOperation.right, Functions.Min, None, Some("x"))
    secondOperation.alias should be("op2")

    val thirdOperation = projections(2).asInstanceOf[Operation]
    thirdOperation.left.asInstanceOf[Number].value should be(35L)
    thirdOperation.operator should be(MathOperators.Multiply)
    verifyField(thirdOperation.right, Functions.Mean, None, Some("x"))
    thirdOperation.alias should be("op3")

    val fourthOperation = projections(3).asInstanceOf[Operation]
    fourthOperation.left.asInstanceOf[Number].value should be(3L)
    fourthOperation.operator should be(MathOperators.Divide)
    fourthOperation.right.asInstanceOf[Number].value should be(4L)
    fourthOperation.alias should be("op4")

    verify(parser.metaStore).searchInSnapshotByRegex(regex)
  }

  test("Projecting operations from different metrics should be parsed ok") {
    val parser = buildParser
    val timerMetric1 = "timer-1"
    val timerMetric2 = "timer-2"
    val regexTimer1 = parser.getCaseInsensitiveRegex(timerMetric1)
    val regexTimer2 = parser.getCaseInsensitiveRegex(timerMetric2)

    when(parser.metaStore.searchInSnapshotByRegex(regexTimer1)).thenReturn(Seq(Metric(timerMetric1, MetricType.Timer)))
    when(parser.metaStore.searchInSnapshotByRegex(regexTimer2)).thenReturn(Seq(Metric(timerMetric2, MetricType.Timer)))

    val queryMax = s"""select x.max + y.min as operation from "$timerMetric1" as x, "$timerMetric2" as y group by time(1m)"""

    val projections = await(parser.parse(queryMax)).projections

    val operation = projections(0).asInstanceOf[Operation]
    operation.operator should be(MathOperators.Plus)
    operation.alias should be("operation")

    verifyField(operation.left, Functions.Max, None, Some("x"))
    verifyField(operation.right, Functions.Min, None, Some("y"))

    verify(parser.metaStore).searchInSnapshotByRegex(regexTimer1)
    verify(parser.metaStore).searchInSnapshotByRegex(regexTimer2)
  }

  test("Query with scalar projection should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val queryScalar = s"""select 1 as positiveValue, -3 as negativeValue, 12.56 as decimalValue from "$metricName" group by time(30s)"""

    val projections = await(parser.parse(queryScalar)).projections

    val number1 = projections(0).asInstanceOf[Number]
    number1.value should be(1L)
    number1.alias should be(Some("positiveValue"))

    val number2 = projections(1).asInstanceOf[Number]
    number2.value should be(-3L)
    number2.alias should be(Some("negativeValue"))

    val number3 = projections(2).asInstanceOf[Number]
    number3.value should be(12.56)
    number3.alias should be(Some("decimalValue"))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)
  }

  test("Select from regex matching some metrics should be parsed ok") {
    val parser = buildParser

    val counterCommonName = "Counter"
    val counter1 = s"$counterCommonName-1"
    val counter2 = s"$counterCommonName-2"
    val regexCommon = s".*$counterCommonName.*"
    val regex = parser.getCaseInsensitiveRegex(regexCommon)

    val metrics = Seq(Metric(counter1, MetricType.Counter), Metric(counter2, MetricType.Counter))
    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(metrics)

    val queryRegex = s"""select * from "$regexCommon" group by time(30s)"""
    val influxCriteria = await(parser.parse(queryRegex))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    influxCriteria.projections.size should be(4)
    verifyField(influxCriteria.projections(0), Functions.Count, None, Some(counter1))
    verifyField(influxCriteria.projections(1), Functions.Cpm, None, Some(counter1))
    verifyField(influxCriteria.projections(2), Functions.Count, None, Some(counter2))
    verifyField(influxCriteria.projections(3), Functions.Cpm, None, Some(counter2))

    influxCriteria.sources.size should be(2)
    verifySource(influxCriteria.sources(0), counter1, None)
    verifySource(influxCriteria.sources(1), counter2, None)
  }

  test("Select with many regex should be parsed ok") {
    val parser = buildParser

    val counterCommonName = "Counter"
    val counter1 = s"$counterCommonName-1"
    val counter2 = s"$counterCommonName-2"
    val regexCounterCommon = s".*$counterCommonName.*"
    val regexCounter = parser.getCaseInsensitiveRegex(regexCounterCommon)

    val timerCommonName = "Timer"
    val timer1 = s"$timerCommonName-1"
    val timer2 = s"$timerCommonName-2"
    val regexTimerCommon = s".*$timerCommonName.*"
    val regexTimer = parser.getCaseInsensitiveRegex(regexTimerCommon)

    val counters = Seq(Metric(counter1, MetricType.Counter), Metric(counter2, MetricType.Counter))
    when(parser.metaStore.searchInSnapshotByRegex(regexCounter)).thenReturn(counters)

    val timers = Seq(Metric(timer1, MetricType.Timer), Metric(timer2, MetricType.Timer))
    when(parser.metaStore.searchInSnapshotByRegex(regexTimer)).thenReturn(timers)

    val queryRegex = s"""select count from "$regexCounterCommon", "$regexTimerCommon" group by time(30s)"""
    val influxCriteria = await(parser.parse(queryRegex))

    verify(parser.metaStore).searchInSnapshotByRegex(regexCounter)
    verify(parser.metaStore).searchInSnapshotByRegex(regexTimer)

    influxCriteria.projections.size should be(4)
    verifyField(influxCriteria.projections(0), Functions.Count, None, Some(counter1))
    verifyField(influxCriteria.projections(1), Functions.Count, None, Some(counter2))
    verifyField(influxCriteria.projections(2), Functions.Count, None, Some(timer1))
    verifyField(influxCriteria.projections(3), Functions.Count, None, Some(timer2))

    influxCriteria.sources.size should be(4)

    verifySource(influxCriteria.sources(0), counter1, None)
    verifySource(influxCriteria.sources(1), counter2, None)
    verifySource(influxCriteria.sources(2), timer1, None)
    verifySource(influxCriteria.sources(3), timer2, None)
  }

  test("Where clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select count(value) from "$metricName" where host = 'aHost' group by time(5m)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Count, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    val stringFilter = influxCriteria.filters(0).asInstanceOf[StringFilter]
    stringFilter.identifier should be("host")
    stringFilter.operator should be(Operators.Eq)
    stringFilter.value should be("aHost")

    verifyGroupBy(influxCriteria.groupBy, 5, TimeUnit.MINUTES)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Where clause with and should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select max(value) from "$metricName" where time >= 1414508614 and time < 1414509500 group by time(5m)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Max, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyTimeFilter(influxCriteria.filters(0), "time", Operators.Gte, 1414508614L)
    verifyTimeFilter(influxCriteria.filters(1), "time", Operators.Lt, 1414509500L)

    verifyGroupBy(influxCriteria.groupBy, 5, TimeUnit.MINUTES)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Where clause with time suffix should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select min(value) from "$metricName" where time >= 1414508614s group by time(30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyTimeFilter(influxCriteria.filters(0), "time", Operators.Gte, 1414508614000L)
  }

  test("Where clauses like (now - 1h) should be parsed ok") {
    val mockedNow = 1414767928000L
    val mockedParser = new InfluxQueryParser() {
      override val metaStore: MetaStore = mock[MetaStore]
      override def now: Long = mockedNow
    }
    val regex = mockedParser.getCaseInsensitiveRegex(metricName)

    when(mockedParser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val criteriaNow = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time > now() group by time(5m)"""))
    verifyTimeFilter(criteriaNow.filters(0), "time", Operators.Gt, mockedNow)

    val criteriaNow20s = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time < now() - 20s group by time(5m)"""))
    verifyTimeFilter(criteriaNow20s.filters(0), "time", Operators.Lt, mockedNow - TimeUnit.SECONDS.toMillis(20))

    val criteriaNow5m = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time <= now() - 5m group by time(5m)"""))
    verifyTimeFilter(criteriaNow5m.filters(0), "time", Operators.Lte, mockedNow - TimeUnit.MINUTES.toMillis(5))

    val criteriaNow3h = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time >= now() - 3h group by time(5m)"""))
    verifyTimeFilter(criteriaNow3h.filters(0), "time", Operators.Gte, mockedNow - TimeUnit.HOURS.toMillis(3))

    val criteriaNow10d = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time >= now() - 10d group by time(5m)"""))
    verifyTimeFilter(criteriaNow10d.filters(0), "time", Operators.Gte, mockedNow - TimeUnit.DAYS.toMillis(10))

    val criteriaNow2w = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time <= now() - 2w group by time(5m)"""))
    verifyTimeFilter(criteriaNow2w.filters(0), "time", Operators.Lte, mockedNow - TimeUnit.DAYS.toMillis(14))

    verify(mockedParser.metaStore, times(6)).searchInSnapshotByRegex(regex)

  }

  test("Between clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select max(value) from "$metricName" where time between 1414508614 and 1414509500s group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Max, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyTimeFilter(influxCriteria.filters(0), "time", Operators.Gte, 1414508614L)
    verifyTimeFilter(influxCriteria.filters(1), "time", Operators.Lte, 1414509500000L)

    verifyGroupBy(influxCriteria.groupBy, 2, TimeUnit.HOURS)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Group by clause by any windows should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    // Configured windows should be parsed ok
    val influxCriteriaResult30s = await(parser.parse(s"""select count(value) as counter from "$metricName" force group by time(30s)"""))
    verifyGroupBy(influxCriteriaResult30s.groupBy, 30, TimeUnit.SECONDS, true)

    val influxCriteriaResult1m = await(parser.parse(s"""select min(value) as counter from "$metricName" group by time(1m)"""))
    verifyGroupBy(influxCriteriaResult1m.groupBy, 1, TimeUnit.MINUTES)

    // Unconfigured window should be parsed ok
    val influxCriteriaResult13s = await(parser.parse(s"""select count from "$metricName" group by time(13s)"""))
    verifyGroupBy(influxCriteriaResult13s.groupBy, 13, TimeUnit.SECONDS)

    // Decimal windows should be truncated
    val influxCriteriaResultDecimal = await(parser.parse(s"""select count from "$metricName" group by time(0.1s)"""))
    verifyGroupBy(influxCriteriaResultDecimal.groupBy, 0, TimeUnit.SECONDS)

    verify(parser.metaStore, times(4)).searchInSnapshotByRegex(regex)
  }

  test("select with fill option should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select mean from "$metricName" group by time(1m) fill(999)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Mean, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyGroupBy(influxCriteria.groupBy, 1, TimeUnit.MINUTES)

    influxCriteria.fillValue should be(Some(999))
  }

  test("Limit clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select p50(value) from "$metricName" group by time(1m) limit 10"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Percentile50, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyGroupBy(influxCriteria.groupBy, 1, TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(10)
  }

  test("Scale should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select max(value) from "$metricName" group by time(1m) scale(-0.2)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Max, None, Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyGroupBy(influxCriteria.groupBy, 1, TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.scale should be(Some(-0.2))
  }

  test("Order clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val influxCriteriaAsc = await(parser.parse(s"""select p80(value) from "$metricName" group by time(1m) order asc"""))
    influxCriteriaAsc.orderAsc should be(true)

    val influxCriteriaDesc = await(parser.parse(s"""select p90(value) from "$metricName" group by time(1m) order desc"""))
    influxCriteriaDesc.orderAsc should be(false)

    verify(parser.metaStore, times(2)).searchInSnapshotByRegex(regex)
  }

  test("Full Influx query should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metricName, MetricType.Timer)))

    val query = s"""select count(value) as counter from "$metricName" where time > 1000 and time <= 5000 and host <> 'aHost' group by time(30s) limit 550 order desc;"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshotByRegex(regex)

    verifyField(influxCriteria.projections(0), Functions.Count, Some("counter"), Some(metricName))

    influxCriteria.sources.size should be(1)
    verifySource(influxCriteria.sources(0), metricName, None)

    verifyTimeFilter(influxCriteria.filters(0), "time", Operators.Gt, 1000L)
    verifyTimeFilter(influxCriteria.filters(1), "time", Operators.Lte, 5000L)

    val filter3 = influxCriteria.filters(2).asInstanceOf[StringFilter]
    filter3.identifier should be("host")
    filter3.operator should be(Operators.Neq)
    filter3.value should be("aHost")

    verifyGroupBy(influxCriteria.groupBy, 30, TimeUnit.SECONDS)

    influxCriteria.limit should be(550)

    influxCriteria.orderAsc should be(false)
  }

  test("Search for an inexistent metric throws exception") {
    val parser = buildParser
    val metricName = "inexistentMetric"
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq.empty[Metric])

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select * from "$metricName" group by time (30s)"""))

      verify(parser.metaStore).searchInSnapshotByRegex(regex)
    }
  }

  test("Query without projection should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select from "$metricName"""") }
  }

  test("Query without from clause should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse("select max(value) ") }
  }

  test("Query without table should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse("select max(value) from") }
  }

  test("Query using a table alias that dont exist should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select a.max from "$metricName" group by time (30s)""") }
  }

  test("Query with unclosed string literal should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select max(value) from "$metricName" where host = 'host""") }
  }

  test("Query with unclosed parenthesis should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select max(value) from "$metricName" group by time(30s""") }
  }

  test("Query with invalid time now expression should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select max(value) from "$metricName" where time  > now() - 1j group by time(30s)""") }
  }

  test("Select * with other projection should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select * max from "$metricName" group by time(30s)""") }
  }

  test("Select an invalid field for a counter should fail") {
    verifyQueryFail(metricName, s"""select max(value) from "$metricName" group by time(30s)""", MetricType.Counter)
  }

  test("Select an invalid operator should fail") {
    verifyQueryFail(metricName, s"""select max(value) & 3 from "$metricName" group by time(30s)""", MetricType.Timer)
  }

  test("Select with operation without operator should fail") {
    verifyQueryFail(metricName, s"""select max 3 from "$metricName" group by time(30s)""", MetricType.Timer)
  }

  test("Select with unknown order should fail") {
    verifyQueryFail(metricName, s"""select * from "$metricName" group by time(30s) order inexistentOrder""")
  }

  test("Select with invalid percentile function should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse(s"""select percentiles(12) from "$metricName" group by time(30s)""") }
  }

  test("Repeting table alias should fail") {
    verifyQueryFail(metricName, s"""select * from "$metricName" as x, "$metricName" as x group by time(30s)""")
  }

  test("Projection using inexistent table alias should fail") {
    verifyQueryFail(metricName, s"""select y.count from "$metricName" as x group by time(30s)""")
  }

  test("Operation using inexistent table alias should fail") {
    verifyQueryFail(metricName, s"""select y.count + x.max as operation from "$metricName" as x group by time(30s)""")
    verifyQueryFail(metricName, s"""select x.count + y.max as operation from "$metricName" as x group by time(30s)""")
  }

  private def verifyQueryFail(metric: String, query: String, metricType: String = MetricType.Timer) = {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metric)

    when(parser.metaStore.searchInSnapshotByRegex(regex)).thenReturn(Seq(Metric(metric, metricType)))

    intercept[UnsupportedOperationException] {
      await(parser.parse(query))
      verify(parser.metaStore).searchInSnapshotByRegex(regex)
    }
  }

  private def verifyField(projection: Projection, expectedFunction: Functions.Function, expectedFieldAlias: Option[String], expectedTableAlias: Option[String]) = {
    projection.asInstanceOf[Field].name should be(expectedFunction.name)
    projection.asInstanceOf[Field].alias should be(expectedFieldAlias)
    projection.asInstanceOf[Field].tableId should be(expectedTableAlias)
  }

  private def verifySource(source: Source, expectedMetricName: String, expectedTableAlias: Option[String]) = {
    source.metric.name should be(expectedMetricName)
    source.alias should be(expectedTableAlias)
  }

  private def verifyTimeFilter(filter: Filter, expectedIdentifier: String, expectedOperator: String, millis: Long) = {
    val timeFilter = filter.asInstanceOf[TimeFilter]
    timeFilter.identifier should be(expectedIdentifier)
    timeFilter.operator should be(expectedOperator)
    timeFilter.value should be(millis)
  }

  private def verifyGroupBy(groupBy: GroupBy, expectedDurationLength: Int, expectedDurationUnit: TimeUnit, expectedForced: Boolean = false) = {
    groupBy.duration.length should be(expectedDurationLength)
    groupBy.duration.unit should be(expectedDurationUnit)
    groupBy.forceResolution should be(expectedForced)
  }

  private def await[T](f: ⇒ Future[T]): T = Await.result(f, 10 seconds)
}