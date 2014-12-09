package com.despegar.khronus.store

import com.despegar.khronus.model.{MetricType, CounterSummary, Metric, StatisticSummary}
import com.despegar.khronus.util.BaseIntegrationTest
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._

class CassandraCounterSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{
  override val tableNames: Seq[String] = CassandraCounterSummaryStore.windowDurations.map(duration => CassandraCounterSummaryStore.tableName(duration))

  test("An CounterSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = new CounterSummary(22L, 250L)
    val secondSummary = new CounterSummary(30L, 3000L)
    val summaries = Seq(summary, secondSummary)
    await { CassandraCounterSummaryStore.store(Metric("testMetric", MetricType.Counter), 30 seconds, summaries) }

    val bucketsFromCassandra = await { CassandraCounterSummaryStore.sliceUntilNow(Metric("testMetric", MetricType.Counter), 30 seconds) }

    bucketsFromCassandra(0) shouldEqual summary
    bucketsFromCassandra(1) shouldEqual secondSummary
  }

  test("Slice without results") {
    val bucketsFromCassandra = await { CassandraCounterSummaryStore.sliceUntilNow(Metric("inexistent metric", MetricType.Counter), 30 seconds) }

    bucketsFromCassandra.isEmpty shouldBe true
  }

}
