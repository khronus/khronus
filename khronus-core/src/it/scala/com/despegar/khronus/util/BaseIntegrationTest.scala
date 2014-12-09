package com.despegar.khronus.util

import com.despegar.khronus.store.{CassandraSupport, CassandraBuckets, CassandraMeta, CassandraSummaries}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  def tableNames: Seq[String] = Seq.empty[String]

  override def beforeAll = {
    CassandraMeta.initialize
    CassandraBuckets.initialize
    CassandraSummaries.initialize

    truncateTables(CassandraBuckets)
    truncateTables(CassandraSummaries)
    truncateTables(CassandraMeta)
  }

  after {
    truncateTables(CassandraBuckets)
    truncateTables(CassandraSummaries)
    truncateTables(CassandraMeta)
  }

  override protected def afterAll() = { }

  def await[T](f: => Future[T]): T = Await.result(f, 20 seconds)

  def truncateTables(cassandra: CassandraSupport) = Try {
    tableNames.foreach(table => cassandra.truncate(table))
  }

}
