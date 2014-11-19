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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.despegar.metrik.influx.parser.InfluxQueryParser
import com.despegar.metrik.influx.service.{ InfluxEndpoint, InfluxSeries }
import com.despegar.metrik.model.{ CounterSummary, Functions, Metric, StatisticSummary, _ }
import com.despegar.metrik.store.{ Slice, _ }
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.mockito.{ Mockito, Matchers ⇒ MockitoMatchers }
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class InfluxQueryResolverSpec extends FunSuite with BeforeAndAfter with Matchers with MockitoSugar with InfluxQueryResolver with InfluxEndpoint {
  override implicit def actorRefFactory = ActorSystem("TestSystem", ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      | }
    """.stripMargin))

  val metaStoreMock = mock[MetaStore]

  override lazy val metaStore = metaStoreMock
  override lazy val getStatisticSummaryStore = mock[SummaryStore[StatisticSummary]]
  override lazy val getCounterSummaryStore = mock[SummaryStore[CounterSummary]]
  override lazy val now = System.currentTimeMillis()

  override lazy val parser: InfluxQueryParser = new InfluxQueryParser() {
    override val metaStore: MetaStore = metaStoreMock

    override def getConfiguredWindows(metricType: String): Seq[FiniteDuration] = {
      Seq(FiniteDuration(30, TimeUnit.SECONDS),
        FiniteDuration(5, TimeUnit.MINUTES))
    }
  }

  before {
    Mockito.reset(metaStore, getStatisticSummaryStore, getCounterSummaryStore)
  }

  test("Search for a invalid metric type throws exception") {
    val metricName = "counterMetric"
    val query = s"""select count(value) from "$metricName" group by time (30s)"""

    val counterMetric = Metric(metricName, "unknownType")
    when(metaStore.getFromSnapshot).thenReturn(Seq(counterMetric))

    intercept[UnsupportedOperationException] {
      Await.result(search(query), 2 seconds)

      verify(metaStore).getFromSnapshot
    }
  }

  test("Select a valid field for a counter metric returns influx series ok") {
    val metricName = "counterMetric"
    val query = s"""select count(value) from "$metricName" group by time (30s)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Counter)

    val summary1 = CounterSummary(System.currentTimeMillis() - 1000, 100L)
    val summary2 = CounterSummary(System.currentTimeMillis(), 80L)
    when(getCounterSummaryStore.readAll(FiniteDuration(30, TimeUnit.SECONDS), metricName, Slice(-1L, now), Int.MaxValue)).thenReturn(Future { Seq(summary1, summary2) })

    val results = Await.result(search(query), 2 seconds)

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getCounterSummaryStore).readAll(FiniteDuration(30, TimeUnit.SECONDS), metricName, Slice(-1L, now), Int.MaxValue)

    results.size should be(1)
    results(0).name should be(metricName)

    results(0).columns(0) should be(InfluxQueryResolver.influxTimeKey)
    results(0).columns(1) should be(Functions.Count.name)

    results(0).points(0)(0) should be(summary1.timestamp.ms)
    results(0).points(0)(1) should be(summary1.count)

    results(0).points(1)(0) should be(summary2.timestamp.ms)
    results(0).points(1)(1) should be(summary2.count)
  }

  test("Select * for a valid counter metric returns influx series ok") {
    val metricName = "counterMetric"
    val query = s"""select * from "$metricName" group by time (5m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Counter)

    val summary = CounterSummary(System.currentTimeMillis() - 1000, 100L)
    when(getCounterSummaryStore.readAll(FiniteDuration(5, TimeUnit.MINUTES), metricName, Slice(-1L, now), Int.MaxValue)).thenReturn(Future { Seq(summary) })

    val results = Await.result(search(query), 2 seconds)

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getCounterSummaryStore).readAll(FiniteDuration(5, TimeUnit.MINUTES), metricName, Slice(-1L, now), Int.MaxValue)

    results.size should be(1)
    assertInfluxSeries(results(0), metricName, Functions.Count.name, summary.timestamp.ms, summary.count)
  }

  test("Select * for a valid histogram metric returns influx series ok") {
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - 3600000L
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (5m) limit 10 order desc"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)

    val summary = StatisticSummary(System.currentTimeMillis(), 50L, 80L, 90L, 95L, 99L, 999L, 3L, 1000L, 100L, 200L)
    when(getStatisticSummaryStore.readAll(FiniteDuration(5, TimeUnit.MINUTES), metricName, Slice(to, from, true), 10)).thenReturn(Future { Seq(summary) })

    val results = Await.result(search(query), 2 seconds)

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(FiniteDuration(5, TimeUnit.MINUTES), metricName, Slice(to, from, true), 10)

    // Select * makes 1 series for each function
    results.size should be(10)

    val sortedResults = results.sortBy(_.columns(1))
    assertInfluxSeries(sortedResults(0), metricName, Functions.Avg.name, summary.timestamp.ms, summary.avg)
    assertInfluxSeries(sortedResults(1), metricName, Functions.Count.name, summary.timestamp.ms, summary.count)
    assertInfluxSeries(sortedResults(2), metricName, Functions.Max.name, summary.timestamp.ms, summary.max)
    assertInfluxSeries(sortedResults(3), metricName, Functions.Min.name, summary.timestamp.ms, summary.min)
    assertInfluxSeries(sortedResults(4), metricName, Functions.Percentile50.name, summary.timestamp.ms, summary.p50)
    assertInfluxSeries(sortedResults(5), metricName, Functions.Percentile80.name, summary.timestamp.ms, summary.p80)
    assertInfluxSeries(sortedResults(6), metricName, Functions.Percentile90.name, summary.timestamp.ms, summary.p90)
    assertInfluxSeries(sortedResults(7), metricName, Functions.Percentile95.name, summary.timestamp.ms, summary.p95)
    assertInfluxSeries(sortedResults(8), metricName, Functions.Percentile99.name, summary.timestamp.ms, summary.p99)
    assertInfluxSeries(sortedResults(9), metricName, Functions.Percentile999.name, summary.timestamp.ms, summary.p999)
  }

  private def assertInfluxSeries(series: InfluxSeries, expectedName: String, expectedFunction: String, expectedMillis: Long, expectedValue: Long) = {
    series.name should be(expectedName)
    series.columns(0) should be(InfluxQueryResolver.influxTimeKey)
    series.columns(1) should be(expectedFunction)
    series.points(0)(0) should be(expectedMillis)
    series.points(0)(1) should be(expectedValue)
  }
}