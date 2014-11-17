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
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.connectionpool.OperationResult
import scala.concurrent.duration._

trait BaseInfluxIntegrationSpec extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  override def beforeAll = {
    Cassandra.initialize
    InfluxDashboardResolver.initialize

    truncateColumnFamilies
  }

  after {
    truncateColumnFamilies
  }

  override protected def afterAll() = {
    //Metrik.system.shutdown()
  }

  def await[T](f: => Future[T]):T = Await.result(f, 3 seconds)

  def truncateColumnFamilies = Try {
    columnFamilies.foreach(cf => Cassandra.keyspace.truncateColumnFamily(cf))
  }

  def columnFamilies = Seq.empty[ColumnFamily[String, String]]
}