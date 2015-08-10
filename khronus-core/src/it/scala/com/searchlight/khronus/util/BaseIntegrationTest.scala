package com.searchlight.khronus.util

import com.searchlight.khronus.store.{CassandraKeyspace, Buckets, Meta, Summaries}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  System.setProperty("config.resource", "application-it-test.conf")

  def tableNames: Seq[String] = Seq.empty[String]

  override def beforeAll = {
    Meta.initialize
    Buckets.initialize
    Summaries.initialize

    truncateTables(Buckets)
    truncateTables(Summaries)
    truncateTables(Meta)
  }

  after {
    truncateTables(Buckets)
    truncateTables(Summaries)
    truncateTables(Meta)
  }

  override protected def afterAll() = { }

  def await[T](f: => Future[T]): T = Await.result(f, 20 seconds)

  def truncateTables(cassandra: CassandraKeyspace) = Try {
    tableNames.foreach(table => cassandra.truncate(table))
  }

}
