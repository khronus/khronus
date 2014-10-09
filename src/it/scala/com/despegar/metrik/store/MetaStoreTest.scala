package com.despegar.metrik.store

import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.Matchers
import com.despegar.metrik.util.Config
import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.FunSuite
import scala.util.Try
import com.despegar.metrik.model.Metric

class CassandraMetaStoreTest extends FunSuite with BaseIntegrationTest with Config with Matchers {

  test("should store and retrieve metadata for metrics") {
    await { CassandraMetaStore.store("metric1") }
    val metrics = await { CassandraMetaStore.retrieveMetrics }
    metrics shouldEqual Seq("metric1")
  }
  
  override def truncateColumnFamilies = Try {
    Cassandra.keyspace.truncateColumnFamily(CassandraMetaStore.columnFamily)
  }
}