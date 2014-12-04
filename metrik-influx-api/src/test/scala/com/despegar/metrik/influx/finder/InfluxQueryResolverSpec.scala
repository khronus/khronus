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

  override lazy val maxResolution: Int = 1000
  override lazy val minResolution: Int = 700
  override def getConfiguredWindows(metricType: String): Seq[FiniteDuration] = Seq(FiniteDuration(30, TimeUnit.SECONDS), FiniteDuration(5, TimeUnit.MINUTES), FiniteDuration(30, TimeUnit.MINUTES))

  override lazy val parser: InfluxQueryParser = new InfluxQueryParser() {
    override val metaStore: MetaStore = metaStoreMock
  }

  before {
    Mockito.reset(metaStore, getStatisticSummaryStore, getCounterSummaryStore)
  }

  test("Search for a invalid metric type throws exception") {
    val metricName = "counterMetric"
    val query = s"""select * from "$metricName" group by time (30s)"""

    val counterMetric = Metric(metricName, "unknownType")
    when(metaStore.getFromSnapshot).thenReturn(Map(counterMetric -> Timestamp(1)))

    intercept[UnsupportedOperationException] {
      await(search(query))
      verify(metaStore).getFromSnapshot
    }
  }

  test("Select a valid field for a counter metric returns influx series ok") {
    val metricName = "counterMetric"
    val query = s"""select count(value) from "$metricName" group by time (30m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Counter)

    val summary1 = CounterSummary(System.currentTimeMillis() - 1000, 100L)
    val summary2 = CounterSummary(System.currentTimeMillis(), 80L)
    when(getCounterSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(-1L, now), true, Int.MaxValue)).thenReturn(Future { Seq(summary1, summary2) })

    val results = await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getCounterSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(-1L, now), true, Int.MaxValue)

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
    val query = s"""select * from "$metricName" group by time (30m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Counter)

    val summary = CounterSummary(System.currentTimeMillis() - 1000, 100L)
    when(getCounterSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(-1L, now), true, Int.MaxValue)).thenReturn(Future { Seq(summary) })

    val results = await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getCounterSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(-1L, now), true, Int.MaxValue)

    results.size should be(1)
    assertInfluxSeries(results(0), metricName, Functions.Count.name, summary.timestamp.ms, summary.count)
  }

  test("Select * for a valid histogram metric returns influx series ok") {
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(80, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (5m) limit 10 order desc"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)

    val summary = StatisticSummary(System.currentTimeMillis(), 50L, 80L, 90L, 95L, 99L, 999L, 3L, 1000L, 100L, 200L)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), false, 10)).thenReturn(Future { Seq(summary) })

    val results = await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), false, 10)

    // Select * makes 1 series for each function
    results.size should be(10)

    val sortedResults = results.sortBy(_.columns(1))

    assertInfluxSeries(sortedResults(0), metricName, Functions.Count.name, summary.timestamp.ms, summary.count)
    assertInfluxSeries(sortedResults(1), metricName, Functions.Max.name, summary.timestamp.ms, summary.max)
    assertInfluxSeries(sortedResults(2), metricName, Functions.Mean.name, summary.timestamp.ms, summary.mean)
    assertInfluxSeries(sortedResults(3), metricName, Functions.Min.name, summary.timestamp.ms, summary.min)
    assertInfluxSeries(sortedResults(4), metricName, Functions.Percentile50.name, summary.timestamp.ms, summary.p50)
    assertInfluxSeries(sortedResults(5), metricName, Functions.Percentile80.name, summary.timestamp.ms, summary.p80)
    assertInfluxSeries(sortedResults(6), metricName, Functions.Percentile90.name, summary.timestamp.ms, summary.p90)
    assertInfluxSeries(sortedResults(7), metricName, Functions.Percentile95.name, summary.timestamp.ms, summary.p95)
    assertInfluxSeries(sortedResults(8), metricName, Functions.Percentile99.name, summary.timestamp.ms, summary.p99)
    assertInfluxSeries(sortedResults(9), metricName, Functions.Percentile999.name, summary.timestamp.ms, summary.p999)
  }

  test("Select with a configured resolution between configured limits returns the desired window") {
    // 80 h  / 5 minutes = 960 points (ok, between 700 and 1000)

    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(80, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (5m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)
  }

  test("Select with unconfigured time window should use the nearest window") {
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)

    var from = to - FiniteDuration(8, HOURS).toMillis
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })
    await(search(s"""select * from "$metricName" where time >= $from and time <=  $to group by time(10s)"""))
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)

    from = to - FiniteDuration(80, HOURS).toMillis
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })
    await(search(s"""select * from "$metricName" where time >= $from and time <=  $to group by time(6m)"""))
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)

    from = to - FiniteDuration(500, HOURS).toMillis
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })
    await(search(s"""select * from "$metricName" where time >= $from and time <=  $to group by time(5h)"""))
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)

    verify(metaStore, times(6)).getMetricType(metricName)

  }

  test("Select with a bad resolution adjust it to the best configured window") {
    // 80 h  / 30 minutes = 160 points (resolution too bad! Adjust it to 5 minutes in order to have 960 points, between 700 and 1000)
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(80, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (30m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)
  }

  test("Select with a very high resolution adjust it to the best configured window") {
    // 80 h  / 30 seconds = 9600 points (Too much points! Adjust it to 5 minutes in order to have 960 points, between 700 and 1000)
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(80, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (30s)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(5, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)
  }

  test("Select with a very high resolution forced should use the nearest window") {
    // 80 h  / 30 seconds = 9600 points (Too much points!)
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(80, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to force group by time (30s)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)
  }

  test("Select without time bounds adjust window to the lowest configured resolution") {
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = -1L
    val query = s"""select * from "$metricName" where time <=  $to group by time (5m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)
  }

  test("Select with a very high resolution returns the lowest configured resolution outside boundaries") {
    // 1000 h  / 5 minutes = 12000 points. Adjust to the lowest configured window => 1000h / 30m = 2000 points. Returns 30m, even when this window is outside boundaries (between 700 and 1000 points)
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(1000, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <=  $to group by time (5m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.MINUTES), Slice(from, to), true, Int.MaxValue)
  }

  test("Select with a very bad resolution returns the highest configured resolution outside boundaries") {
    // 1h  / 5 minutes = 12 points. Adjust to the lowest configured window => 1h / 30s = 120 points. Returns 30s, even when this window is outside boundaries (between 700 and 1000 points)
    val metricName = "histogramMetric"
    val to = System.currentTimeMillis()
    val from = to - FiniteDuration(1, HOURS).toMillis
    val query = s"""select * from "$metricName" where time >= $from and time <= $to group by time (5m)"""

    when(metaStore.getMetricType(metricName)).thenReturn(MetricType.Timer)
    when(getStatisticSummaryStore.readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)).thenReturn(Future { Seq() })

    await(search(query))

    verify(metaStore, times(2)).getMetricType(metricName)
    verify(getStatisticSummaryStore).readAll(metricName, FiniteDuration(30, TimeUnit.SECONDS), Slice(from, to), true, Int.MaxValue)
  }

  private def assertInfluxSeries(series: InfluxSeries, expectedName: String, expectedFunction: String, expectedMillis: Long, expectedValue: Long) = {
    series.name should be(expectedName)
    series.columns(0) should be(InfluxQueryResolver.influxTimeKey)
    series.columns(1) should be(expectedFunction)
    series.points(0)(0) should be(expectedMillis)
    series.points(0)(1) should be(expectedValue)
  }

  private def await[T](f: ⇒ Future[T]): T = Await.result(f, 2 seconds)

}