package com.despegar.metrik.util

import com.despegar.metrik.store.{CassandraStatisticSummaryStore, Cassandra}
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{BeforeAndAfter, FunSuite, BeforeAndAfterAll}
import scala.collection.JavaConverters._
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

 /* override def beforeAll = {
    try {
      println("start BEFORE ALL")
      createKeyspace
      println("beforeAll - keyspace created")
    } catch {
      case e: Exception => println("beforeAll createKeyspace fail. continue")
    }

    try {
      truncateColumnFamilies
      println("beforeAll - column families truncated")
    } catch {
      case e: Exception => println("beforeAll truncateColumnFamilies fail. continue")
    }

    try {
      createColumnFamilies
      println("beforeAll - column families created")
    } catch {
      case e: Exception => println("beforeAll createColumnFamilies fail. continue")
    }
  }

  override def afterAll = {
    println("AFTER ALL")
    //dropKeyspace
  }*/

  override def beforeAll = {
    Try {
      dropKeyspace
    }

    Try {
      createKeyspace
    }

    Try {
      createColumnFamilies
    }

  }


  private def createKeyspace = {
    val keyspace = Map("strategy_options" -> Map("replication_factor" -> "1").asJava, "strategy_class" -> "SimpleStrategy")
    val result = Cassandra.keyspace.createKeyspaceIfNotExists(keyspace.asJava).getResult();
    println(s"Result from create keyspace $result")
    result.getSchemaId()
  }

  def createColumnFamilies

  private def dropKeyspace = Cassandra.keyspace.dropKeyspace().getResult


  after {
    Try {
      truncateColumnFamilies
    }
  }

  private def truncateColumnFamilies = foreachColumnFamily { Cassandra.keyspace.truncateColumnFamily(_) }

  def foreachColumnFamily(f: ColumnFamily[String,java.lang.Long] => OperationResult[_])
}
