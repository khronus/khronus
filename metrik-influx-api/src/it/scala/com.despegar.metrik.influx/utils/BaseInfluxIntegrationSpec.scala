/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.influx.finder

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import com.despegar.metrik.store.Cassandra
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.concurrent.duration._

trait BaseInfluxIntegrationSpec extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  def tableNames: Seq[String] = Seq.empty[String]

  override def beforeAll = {
    Cassandra.initialize
    InfluxDashboardResolver.initialize

    truncateTables
  }

  after {
    truncateTables
  }

  override protected def afterAll() = { }

  def await[T](f: => Future[T]):T = Await.result(f, 3 seconds)

  def truncateTables = Try {
    tableNames.foreach(table => Cassandra.truncate(table))
  }

}