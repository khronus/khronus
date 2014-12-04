package com.despegar.metrik.store

import com.despegar.metrik.model.{Timestamp, StatisticSummary, Metric}
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{Matchers,  FunSuite}
import scala.concurrent.duration._

class CassandraStatisticSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{

  test("An StatisticSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = StatisticSummary(Timestamp(1417639860000l),47903,47903,47903,47903,47903,47903,47872,47903,1,47888)
    val summaries = Seq(summary)
    await { CassandraStatisticSummaryStore.store(Metric("testMetric","histogram"), 30 seconds, summaries) }
    val bucketsFromCassandra = await { CassandraStatisticSummaryStore.sliceUntilNow(Metric("testMetric","histogram"), 30 seconds) }
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  override def foreachColumnFamily(f: ColumnFamily[String,_] => OperationResult[_]) = {
    CassandraStatisticSummaryStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }

}
