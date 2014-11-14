package com.despegar.metrik.util

import com.despegar.metrik.model.UniqueTimestamp
import com.despegar.metrik.store.Cassandra
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  override def beforeAll = {
    Cassandra initialize

    truncateColumnFamilies
  }

  after {
    truncateColumnFamilies
  }

  override protected def afterAll() = {
  }

  def await[T](f: => Future[T]): T = Await.result(f, 10 seconds)

  def truncateColumnFamilies = Try {
    foreachColumnFamily(cf => Cassandra.keyspace.truncateColumnFamily(cf))
  }

  def foreachColumnFamily(f: ColumnFamily[String, _] => OperationResult[_]) = {}
}
