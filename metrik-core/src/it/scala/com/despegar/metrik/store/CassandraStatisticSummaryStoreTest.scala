package com.despegar.metrik.store

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.{Matchers,  FunSuite}
import scala.concurrent.duration._
import com.despegar.metrik.model.Metric

class CassandraStatisticSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{
  override val tableNames: Seq[String] = CassandraStatisticSummaryStore.windowDurations.map(duration => CassandraStatisticSummaryStore.tableName(duration))

  test("An StatisticSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = StatisticSummary(1,50,50,50,90,99,100,50,100,20,50)
    val summaries = Seq(summary)
    await { CassandraStatisticSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }
    val bucketsFromCassandra = await { CassandraStatisticSummaryStore.sliceUntilNow(Metric("testMetric","histogram"), 30 seconds) }
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  test("Read should return only the right summary") {
    val earlierSummary = StatisticSummary(1000,50,50,50,90,99,100,50,100,20,50)
    val onIntervalSummary = StatisticSummary(2000,30,40,45,80,90,98,30,90,45,77)
    val laterSummary = StatisticSummary(3000,11,22,33,44,55,66,77,88,99,80)

    val summaries = Seq(earlierSummary, onIntervalSummary, laterSummary)
    await { CassandraStatisticSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }

    val bucketsFromCassandra = await { CassandraStatisticSummaryStore.readAll("testMetric", 30 seconds, Slice(1500, 2500)) }
    bucketsFromCassandra.size shouldEqual 1
    bucketsFromCassandra(0) shouldEqual onIntervalSummary
  }

  test("Reading with descending order should returns timestamp desc") {
    val earlierSummary = StatisticSummary(1,50,50,50,90,99,100,50,100,20,50)
    val laterSummary = StatisticSummary(3,80,81,82,83,84,85,86,87,88,89)
    val summaries = Seq(earlierSummary, laterSummary)
    await { CassandraStatisticSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }

    val bucketsFromCassandra = await { CassandraStatisticSummaryStore.readAll("testMetric", 30 seconds, Slice(0, 100), false) }

    bucketsFromCassandra(0) shouldEqual laterSummary
    bucketsFromCassandra(1) shouldEqual earlierSummary
  }


}
