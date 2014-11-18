package com.despegar.metrik.store

import com.despegar.metrik.model.{MetricType, CounterSummary, Metric, StatisticSummary}
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._

class CassandraCounterSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{

  test("An CounterSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = new CounterSummary(22L, 250L)
    val summaries = Seq(summary)
    await { CassandraCounterSummaryStore.store(Metric("testMetric", MetricType.Counter), 30 seconds, summaries) }
    val bucketsFromCassandra = await { CassandraCounterSummaryStore.sliceUntilNow(Metric("testMetric", MetricType.Counter), 30 seconds) }
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  override def foreachColumnFamily(f: ColumnFamily[String,_] => OperationResult[_]) = {
    CassandraCounterSummaryStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }
}
