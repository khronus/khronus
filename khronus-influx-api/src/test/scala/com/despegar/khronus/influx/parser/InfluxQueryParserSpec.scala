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

package com.despegar.khronus.influx.parser

import com.despegar.khronus.model._
import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.Some
import scala.concurrent.{ Await, Future }
import com.despegar.khronus.store.MetaStore
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

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select count(value) from "$metricName" group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be(Functions.Count.name)
    resultedField.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select with many projections should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select mean max as maxValue min(value) from "$metricName" group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    influxCriteria.projections.size should be(3)

    val firstProjection = influxCriteria.projections(0)
    firstProjection.name should be(Functions.Mean.name)
    firstProjection.alias should be(None)

    val secondProjection = influxCriteria.projections(1)
    secondProjection.name should be(Functions.Max.name)
    secondProjection.alias should be(Some("maxValue"))

    val thirdProjection = influxCriteria.projections(2)
    thirdProjection.name should be(Functions.Min.name)
    thirdProjection.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.filters should be(Nil)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select * for a timer should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select * from "$metricName" group by time (30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    influxCriteria.projections.size should be(10)
    val sortedProjections = influxCriteria.projections.sortBy(_.name)

    sortedProjections(0).name should be(Functions.Count.name)
    sortedProjections(1).name should be(Functions.Max.name)
    sortedProjections(2).name should be(Functions.Mean.name)
    sortedProjections(3).name should be(Functions.Min.name)
    sortedProjections(4).name should be(Functions.Percentile50.name)
    sortedProjections(5).name should be(Functions.Percentile80.name)
    sortedProjections(6).name should be(Functions.Percentile90.name)
    sortedProjections(7).name should be(Functions.Percentile95.name)
    sortedProjections(8).name should be(Functions.Percentile99.name)
    sortedProjections(9).name should be(Functions.Percentile999.name)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("select * for a counter should be parsed ok") {

    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Counter)) })

    val query = s"""select * from "$metricName" group by time (30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    influxCriteria.projections.size should be(1)
    influxCriteria.projections(0).name should be(Functions.Count.name)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Select fields for a timer should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val queryMax = s"""select max from "$metricName" group by time(1m)"""
    val resultedFieldMax = await(parser.parse(queryMax)).projections(0)
    resultedFieldMax.name should be(Functions.Max.name)

    val queryMin = s"""select min from "$metricName" group by time(1m)"""
    val resultedFieldMin = await(parser.parse(queryMin)).projections(0)
    resultedFieldMin.name should be(Functions.Min.name)

    val queryMean = s"""select mean from "$metricName" group by time(1m)"""
    val resultedFieldMean = await(parser.parse(queryMean)).projections(0)
    resultedFieldMean.name should be(Functions.Mean.name)

    val queryCount = s"""select count from "$metricName" group by time(1m)"""
    val resultedFieldCount = await(parser.parse(queryCount)).projections(0)
    resultedFieldCount.name should be(Functions.Count.name)

    val query50 = s"""select p50 from "$metricName" group by time(30s)"""
    val resultedField50 = await(parser.parse(query50)).projections(0)
    resultedField50.name should be(Functions.Percentile50.name)

    val query80 = s"""select p80 from "$metricName" group by time(30s)"""
    val resultedField80 = await(parser.parse(query80)).projections(0)
    resultedField80.name should be(Functions.Percentile80.name)

    val query90 = s"""select p90 from "$metricName" group by time(30s)"""
    val resultedField90 = await(parser.parse(query90)).projections(0)
    resultedField90.name should be(Functions.Percentile90.name)

    val query95 = s"""select p95 from "$metricName" group by time(30s)"""
    val resultedField95 = await(parser.parse(query95)).projections(0)
    resultedField95.name should be(Functions.Percentile95.name)

    val query99 = s"""select p99 from "$metricName" group by time(30s)"""
    val resultedField99 = await(parser.parse(query99)).projections(0)
    resultedField99.name should be(Functions.Percentile99.name)

    val query999 = s"""select p999 from "$metricName" group by time(30s)"""
    val resultedField999 = await(parser.parse(query999)).projections(0)
    resultedField999.name should be(Functions.Percentile999.name)

    verify(parser.metaStore, times(10)).searchInSnapshot(regex)
  }

  test("Select fields for a counter should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Counter)) })

    val queryCounter = s"""select count(value) from "$metricName" group by time(1m)"""
    val resultedFieldCounter = await(parser.parse(queryCounter)).projections(0)

    verify(parser.metaStore).searchInSnapshot(regex)

    resultedFieldCounter.name should be(Functions.Count.name)
  }

  test("All Percentiles function should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val queryAllPercentiles = s"""select percentiles from "$metricName" group by time(30s)"""
    val projections = await(parser.parse(queryAllPercentiles)).projections

    verify(parser.metaStore).searchInSnapshot(regex)

    projections.size should be(6)

    projections(0).name should be(Functions.Percentile50.name)
    projections(1).name should be(Functions.Percentile80.name)
    projections(2).name should be(Functions.Percentile90.name)
    projections(3).name should be(Functions.Percentile95.name)
    projections(4).name should be(Functions.Percentile99.name)
    projections(5).name should be(Functions.Percentile999.name)
  }

  test("Select from regex matching some metrics should be parsed ok") {
    val parser = buildParser

    val counterCommonName = "Counter"
    val counter1 = s"$counterCommonName-1"
    val counter2 = s"$counterCommonName-2"
    val regexCommon = s".*$counterCommonName.*"
    val regex = parser.getCaseInsensitiveRegex(regexCommon)

    val metrics = Seq(Metric(counter1, MetricType.Counter), Metric(counter2, MetricType.Counter))
    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future(metrics))

    val queryRegex = s"""select * from "$regexCommon" group by time(30s)"""
    val influxCriteria = await(parser.parse(queryRegex))

    verify(parser.metaStore).searchInSnapshot(regex)

    influxCriteria.projections.size should be(1)
    influxCriteria.projections(0).name should be(Functions.Count.name)

    influxCriteria.tables.size should be(2)
    influxCriteria.tables(0).name should be(counter1)
    influxCriteria.tables(1).name should be(counter2)
  }

  test("Some Percentiles function should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val queryPercentiles = s"""select percentiles(80 99 50) from "$metricName" group by time(30s)"""
    val projections = await(parser.parse(queryPercentiles)).projections

    verify(parser.metaStore).searchInSnapshot(regex)

    projections.size should be(3)

    projections(0).name should be(Functions.Percentile80.name)
    projections(1).name should be(Functions.Percentile99.name)
    projections(2).name should be(Functions.Percentile50.name)
  }

  test("Where clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select count(value) from "$metricName" where host = 'aHost' group by time(5m)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be(Functions.Count.name)
    resultedField.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    val stringFilter = influxCriteria.filters(0).asInstanceOf[StringFilter]
    stringFilter.identifier should be("host")
    stringFilter.operator should be(Operators.Eq)
    stringFilter.value should be("aHost")

    influxCriteria.groupBy.duration.length should be(5)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Where clause with and should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select max(value) from "$metricName" where time >= 1414508614 and time < 1414509500 group by time(5m)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be(Functions.Max.name)
    resultedField.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lt)
    filter2.value should be(1414509500L)

    influxCriteria.groupBy.duration.length should be(5)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Where clause with time suffix should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select min(value) from "$metricName" where time >= 1414508614s group by time(30s)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614000L)
  }

  test("Where clauses like (now - 1h) should be parsed ok") {
    val mockedNow = 1414767928000L
    val mockedParser = new InfluxQueryParser() {
      override val metaStore: MetaStore = mock[MetaStore]
      override def now: Long = mockedNow
    }
    val regex = mockedParser.getCaseInsensitiveRegex(metricName)

    when(mockedParser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val criteriaNow = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time > now() group by time(5m)"""))
    val filterNow = criteriaNow.filters(0).asInstanceOf[TimeFilter]
    filterNow.identifier should be("time")
    filterNow.operator should be(Operators.Gt)
    filterNow.value should be(mockedNow)

    val criteriaNow20s = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time < now() - 20s group by time(5m)"""))
    val filterNow20s = criteriaNow20s.filters(0).asInstanceOf[TimeFilter]
    filterNow20s.identifier should be("time")
    filterNow20s.operator should be(Operators.Lt)
    filterNow20s.value should be(mockedNow - TimeUnit.SECONDS.toMillis(20))

    val criteriaNow5m = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time <= now() - 5m group by time(5m)"""))
    val filterNow5m = criteriaNow5m.filters(0).asInstanceOf[TimeFilter]
    filterNow5m.identifier should be("time")
    filterNow5m.operator should be(Operators.Lte)
    filterNow5m.value should be(mockedNow - TimeUnit.MINUTES.toMillis(5))

    val criteriaNow3h = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time >= now() - 3h group by time(5m)"""))
    val filterNow3h = criteriaNow3h.filters(0).asInstanceOf[TimeFilter]
    filterNow3h.identifier should be("time")
    filterNow3h.operator should be(Operators.Gte)
    filterNow3h.value should be(mockedNow - TimeUnit.HOURS.toMillis(3))

    val criteriaNow10d = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time >= now() - 10d group by time(5m)"""))
    val filterNow10d = criteriaNow10d.filters(0).asInstanceOf[TimeFilter]
    filterNow10d.identifier should be("time")
    filterNow10d.operator should be(Operators.Gte)
    filterNow10d.value should be(mockedNow - TimeUnit.DAYS.toMillis(10))

    val criteriaNow2w = await(mockedParser.parse(s"""select mean(value) from "$metricName" where time <= now() - 2w group by time(5m)"""))
    val filterNow2w = criteriaNow2w.filters(0).asInstanceOf[TimeFilter]
    filterNow2w.identifier should be("time")
    filterNow2w.operator should be(Operators.Lte)
    filterNow2w.value should be(mockedNow - TimeUnit.DAYS.toMillis(14))

    verify(mockedParser.metaStore, times(6)).searchInSnapshot(regex)

  }

  test("Between clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select max(value) from "$metricName" where time between 1414508614 and 1414509500s group by time(2h)"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be(Functions.Max.name)
    resultedField.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lte)
    filter2.value should be(1414509500000L)

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.limit should be(Int.MaxValue)
  }

  test("Group by clause by any windows should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    // Configured windows should be parsed ok
    val influxCriteriaResult30s = await(parser.parse(s"""select count(value) as counter from "$metricName" force group by time(30s)"""))
    influxCriteriaResult30s.groupBy.forceResolution should be(true)
    influxCriteriaResult30s.groupBy.duration.length should be(30)
    influxCriteriaResult30s.groupBy.duration.unit should be(TimeUnit.SECONDS)

    val influxCriteriaResult1m = await(parser.parse(s"""select min(value) as counter from "$metricName" group by time(1m)"""))
    influxCriteriaResult1m.groupBy.duration.length should be(1)
    influxCriteriaResult1m.groupBy.duration.unit should be(TimeUnit.MINUTES)

    // Unconfigured window should be parsed ok
    val influxCriteriaResult13s = await(parser.parse(s"""select count from "$metricName" group by time(13s)"""))
    influxCriteriaResult13s.groupBy.duration.length should be(13)
    influxCriteriaResult13s.groupBy.duration.unit should be(TimeUnit.SECONDS)

    // Decimal windows should be truncated
    val influxCriteriaResultDecimal = await(parser.parse(s"""select count from "$metricName" group by time(0.1s)"""))
    influxCriteriaResultDecimal.groupBy.duration.length should be(0)
    influxCriteriaResultDecimal.groupBy.duration.unit should be(TimeUnit.SECONDS)

    verify(parser.metaStore, times(4)).searchInSnapshot(regex)
  }

  test("Limit clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select p50(value) from "$metricName" group by time(1m) limit 10"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be(Functions.Percentile50.name)
    resultedField.alias should be(None)

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(10)
  }

  test("Order clause should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val influxCriteriaAsc = await(parser.parse(s"""select p80(value) from "$metricName" group by time(1m) order asc"""))
    influxCriteriaAsc.orderAsc should be(true)

    val influxCriteriaDesc = await(parser.parse(s"""select p90(value) from "$metricName" group by time(1m) order desc"""))
    influxCriteriaDesc.orderAsc should be(false)

    verify(parser.metaStore, times(2)).searchInSnapshot(regex)
  }

  test("Full Influx query should be parsed ok") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    val query = s"""select count(value) as counter from "$metricName" where time > 1000 and time <= 5000 and host <> 'aHost' group by time(30s) limit 550 order desc;"""
    val influxCriteria = await(parser.parse(query))

    verify(parser.metaStore).searchInSnapshot(regex)

    val resultedField = influxCriteria.projections(0)
    resultedField.name should be("count")
    resultedField.alias should be(Some("counter"))

    influxCriteria.tables.size should be(1)
    influxCriteria.tables(0).name should be(metricName)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gt)
    filter1.value should be(1000L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lte)
    filter2.value should be(5000L)

    val filter3 = influxCriteria.filters(2).asInstanceOf[StringFilter]
    filter3.identifier should be("host")
    filter3.operator should be(Operators.Neq)
    filter3.value should be("aHost")

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.limit should be(550)

    influxCriteria.orderAsc should be(false)
  }

  test("Search for an inexistent metric throws exception") {
    val parser = buildParser
    val metricName = "inexistentMetric"
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq.empty[Metric] })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select * from "$metricName" group by time (30s)"""))

      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Query without projection should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select from "$metricName""""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Query without from clause should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse("select max(value) ") }
  }

  test("Query without table should fail") {
    intercept[UnsupportedOperationException] { buildParser.parse("select max(value) from") }
  }

  test("Query with unclosed string literal should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select max(value) from "$metricName" where host = 'host"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Query with unclosed parenthesis should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select max(value) from "$metricName" group by time(30s"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Query with invalid time now expression should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select max(value) from "$metricName" where time  > now() - 1j group by time(30s)"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Select * with other projection should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select * aValue from "$metricName" group by time(30s)"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Select an invalid field for a counter should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Counter)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select max(value) from "$metricName" group by time(30s)"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Select with unknown order should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select * from "$metricName" group by time(30s) order inexistentOrder"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Select with invalid percentile function should fail") {
    val parser = buildParser
    val regex = parser.getCaseInsensitiveRegex(metricName)

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future { Seq(Metric(metricName, MetricType.Timer)) })

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select percentiles(12) from "$metricName" group by time(30s)"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  test("Select from regex matching different metric types should fail") {
    val parser = buildParser

    val commonName = "Metric"
    val counterMetricName = s"counter$commonName"
    val timerMetricName = s"$commonName-Timer"
    val regexCounter = s".*$commonName.*"
    val regex = parser.getCaseInsensitiveRegex(regexCounter)

    val metrics = Seq(Metric(counterMetricName, MetricType.Counter), Metric(timerMetricName, MetricType.Timer))

    when(parser.metaStore.searchInSnapshot(regex)).thenReturn(Future(metrics))

    intercept[UnsupportedOperationException] {
      await(parser.parse(s"""select percentiles from "$regexCounter" group by time(30s)"""))
      verify(parser.metaStore).searchInSnapshot(regex)
    }
  }

  private def await[T](f: ⇒ Future[T]): T = Await.result(f, 10 seconds)
}