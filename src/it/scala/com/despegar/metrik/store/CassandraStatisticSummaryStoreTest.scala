package com.despegar.metrik.store

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{Matchers,  FunSuite}
import scala.concurrent.duration._

class CassandraStatisticSummaryStoreTest extends FunSuite with BaseIntegrationTest with Matchers{

  test("An StatisticSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = StatisticSummary(1,50,50,50,90,99,100,50,100,20,50)
    val summaries = Seq(summary)
    await { CassandraStatisticSummaryStore.store("testMetric", 30 seconds, summaries) }
    val bucketsFromCassandra = await { CassandraStatisticSummaryStore.sliceUntilNow("testMetric", 30 seconds) }
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  override def foreachColumnFamily(f: ColumnFamily[String,java.lang.Long] => OperationResult[_]) = {
    CassandraStatisticSummaryStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }

}
