package com.despegar.metrik.store

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.Config
import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CassandraStatisticSummaryStoreTest extends FunSuite with BeforeAndAfterAll with Config with Matchers{

  override def beforeAll = {
    createKeyspace
    createColumnFamilies
  }

  override def afterAll = dropKeyspace

  test("An StatisticSummary should be capable of serialize and deserialize from Cassandra") {
    val summary = StatisticSummary(1,50,50,50,90,99,100,50,100,20,50)
    val summaries = Seq(summary)
    CassandraStatisticSummaryStore.store("testMetric", 30 seconds, summaries)
    val bucketsFromCassandra = CassandraStatisticSummaryStore.sliceUntilNow("testMetric", 30 seconds)
    val summaryFromCassandra = bucketsFromCassandra(0)

    summary shouldEqual summaryFromCassandra
  }

  private def createKeyspace = {
    val keyspace = Map("strategy_options" -> Map("replication_factor" -> "1").asJava, "strategy_class" -> "SimpleStrategy")
    val result = Cassandra.keyspace.createKeyspaceIfNotExists(keyspace.asJava).getResult();
    result.getSchemaId()
  }

  private def createColumnFamilies = {
    CassandraStatisticSummaryStore.columnFamilies.values.foreach{ cf =>
      Cassandra.keyspace.createColumnFamily(cf, Map[String,Object]().asJava)
    }
  }

  private def dropKeyspace = Cassandra.keyspace.dropKeyspace()
}
