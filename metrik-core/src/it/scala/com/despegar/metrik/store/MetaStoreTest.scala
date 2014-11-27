package com.despegar.metrik.store

import org.scalatest.Matchers
import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.FunSuite
import scala.util.Try
import com.despegar.metrik.model.Metric

class CassandraMetaStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  test("should store and retrieve metadata for metrics") {
    await { CassandraMetaStore.insert(Metric("metric1","histogram")) }
    val metrics = await { CassandraMetaStore.allMetrics() }
    metrics shouldEqual Seq(Metric("metric1","histogram"))
  }
  
  override def truncateColumnFamilies = Try {
    Cassandra.keyspace.truncateColumnFamily(CassandraMetaStore.columnFamily)
  }



}