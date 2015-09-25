package com.searchlight.khronus.store

import com.searchlight.khronus.model.{Timestamp, HistogramSummary, Metric}
import com.searchlight.khronus.util.{Settings, BaseIntegrationTest}
import org.scalatest.{Matchers,  FunSuite}
import scala.concurrent.duration._

class CassandraHistogramSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{
  override val tableNames: Seq[String] = Settings.Window.WindowDurations.map(duration => Summaries.histogramSummaryStore.tableName(duration))

  test("An StatisticSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = HistogramSummary(Timestamp(1417639860000l),47903,47903,47903,47903,47903,47903,47872,47903,1,47888)
    val summaries = Seq(summary)
    await { Summaries.histogramSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }
    val bucketsFromCassandra = await { Summaries.histogramSummaryStore.sliceUntilNow(Metric("testMetric","histogram"), 30 seconds) }
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  test("Read should return only the right summary") {
    val earlierSummary = HistogramSummary(1000,50,50,50,90,99,100,50,100,20,50)
    val onIntervalSummary = HistogramSummary(2000,30,40,45,80,90,98,30,90,45,77)
    val laterSummary = HistogramSummary(3000,11,22,33,44,55,66,77,88,99,80)

    val summaries = Seq(earlierSummary, onIntervalSummary, laterSummary)
    await { Summaries.histogramSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }

    val bucketsFromCassandra = await { Summaries.histogramSummaryStore.readAll("testMetric", 30 seconds, Slice(1500, 2500)) }
    bucketsFromCassandra.size shouldEqual 1
    bucketsFromCassandra(0) shouldEqual onIntervalSummary
  }

  test("Reading with descending order should returns timestamp desc") {
    val earlierSummary = HistogramSummary(1,50,50,50,90,99,100,50,100,20,50)
    val laterSummary = HistogramSummary(3,80,81,82,83,84,85,86,87,88,89)
    val summaries = Seq(earlierSummary, laterSummary)
    await { Summaries.histogramSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }

    val bucketsFromCassandra = await { Summaries.histogramSummaryStore.readAll("testMetric", 30 seconds, Slice(0, 100), false) }

    bucketsFromCassandra(0) shouldEqual laterSummary
    bucketsFromCassandra(1) shouldEqual earlierSummary
  }


}
