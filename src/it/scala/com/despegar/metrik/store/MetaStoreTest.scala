package com.despegar.metrik.store

import org.scalatest.Matchers
import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.FunSuite
import scala.util.Try

class CassandraMetaStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  test("should store and retrieve metadata for metrics") {
    await { CassandraMetaStore.insert("metric1") }
    val metrics = await { CassandraMetaStore.retrieveMetrics }
    metrics shouldEqual Seq("metric1")
  }
  
  override def truncateColumnFamilies = Try {
    Cassandra.keyspace.truncateColumnFamily(CassandraMetaStore.columnFamily)
  }
}