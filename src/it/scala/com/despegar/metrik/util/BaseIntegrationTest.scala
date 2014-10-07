package com.despegar.metrik.util

import com.despegar.metrik.store.{CassandraStatisticSummaryStore, Cassandra}
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{BeforeAndAfter, FunSuite, BeforeAndAfterAll}
import scala.collection.JavaConverters._
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  override def beforeAll = {
    createKeyspace

    createColumnFamilies

    truncateColumnFamilies
  }

  after {
    truncateColumnFamilies
  }

  private def createKeyspace = Try {
    val keyspace = Map("strategy_options" -> Map("replication_factor" -> "1").asJava, "strategy_class" -> "SimpleStrategy")
    val result = Cassandra.keyspace.createKeyspaceIfNotExists(keyspace.asJava).getResult();
    result.getSchemaId()
  }

  def createColumnFamilies

  private def dropKeyspace = {
    val result = Cassandra.keyspace.dropKeyspace().getResult
  }

  private def truncateColumnFamilies = Try {
    foreachColumnFamily {
      Cassandra.keyspace.truncateColumnFamily(_)
    }
  }

  def foreachColumnFamily(f: ColumnFamily[String, java.lang.Long] => OperationResult[_])
}
