package com.despegar.metrik.util

import com.despegar.metrik.store.Cassandra
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try

trait BaseIntegrationTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  def tableNames: Seq[String] = Seq.empty[String]

  override def beforeAll = {
    Cassandra initialize

    truncateTables
  }

  after {
   truncateTables
  }

  override protected def afterAll() = { }

  def await[T](f: => Future[T]): T = Await.result(f, 20 seconds)

  def truncateTables = Try {
    tableNames.foreach(table => Cassandra.truncate(table))
  }

}
