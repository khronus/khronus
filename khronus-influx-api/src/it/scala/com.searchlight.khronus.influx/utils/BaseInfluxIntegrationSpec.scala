/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.searchlight.khronus.influx.finder

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.concurrent.duration._
import com.searchlight.khronus.influx.store.CassandraDashboards
import com.searchlight.khronus.store.CassandraCluster

trait BaseInfluxIntegrationSpec extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  System.setProperty("config.resource", "application-it-test.conf")

  def tableNames: Seq[String] = Seq.empty[String]

  override def beforeAll = {

    CassandraDashboards.initialize

    truncateTables
  }

  after {
    truncateTables
  }

  override protected def afterAll() = { }

  def await[T](f: => Future[T]):T = Await.result(f, 3 seconds)

  def truncateTables = Try {
    tableNames.foreach(table => CassandraDashboards.truncate(table))
  }

}