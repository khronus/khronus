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

package com.despegar.metrik.store

import com.despegar.metrik.util.Settings

import scala.collection.JavaConverters._
import scala.util.Try
import com.despegar.metrik.util.log.Logging
import com.datastax.driver.core._
import scala.concurrent.{ Promise, Future }
import com.google.common.util.concurrent.{ FutureCallback, Futures };

object Cassandra extends Logging {

  lazy val settingsCassandra = Settings().Cassandra

  private lazy val poolingOptions = new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, settingsCassandra.MaxConnectionsPerHost)
  private lazy val socketOptions = new SocketOptions().setConnectTimeoutMillis(settingsCassandra.ConnectionTimeout).setReadTimeoutMillis(settingsCassandra.SocketTimeout)

  private lazy val cluster: Cluster = Cluster.builder().
    withClusterName(settingsCassandra.ClusterName).
    addContactPoints(settingsCassandra.Seeds: _*).
    withPort(settingsCassandra.Port).
    withPoolingOptions(poolingOptions).
    withSocketOptions(socketOptions).build();

  lazy val session: Session = cluster.connect(settingsCassandra.Keyspace)

  def initialize = {
    createSchemaIfNotExists

    CassandraMetaStore.initialize
    CassandraHistogramBucketStore.initialize
    CassandraCounterBucketStore.initialize
    CassandraStatisticSummaryStore.initialize
    CassandraCounterSummaryStore.initialize

  }

  private def createSchemaIfNotExists = withSession { session ⇒
    log.info(s"Initializing schema: ${settingsCassandra.Keyspace}")
    session.execute(s"create keyspace if not exists ${settingsCassandra.Keyspace} with replication = {'class':'SimpleStrategy', 'replication_factor':1};");
  }

  private def withSession(f: Session ⇒ Unit): Unit = {
    val session = cluster.connect()
    try {
      f(session)
    } finally {
      log.info("Closing cassandra connection to system keyspace")
      session.close()
    }
  }

  def close: Unit = {
    log.info("Closing cassandra connection")
    session.close()
    cluster.close()
  }

  import scala.language.implicitConversions

  /**
   * Converts a `ResultSetFuture` into a Scala `Future[ResultSet]`
   * @param f ResultSetFuture to convert
   * @return Converted Future
   */
  implicit def resultSetFutureToScala(f: ResultSetFuture): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }

  def truncate(table: String) = Try {
    session.execute(s"truncate $table;");
  }

  // ---------------------------
  /*
  private var context = {
    new AstyanaxContext.Builder().forCluster(settingsCassandra.ClusterName)
      .forKeyspace(settingsCassandra.Keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CassandraConnectionPool")
        .setPort(settingsCassandra.Port)
        .setSocketTimeout(settingsCassandra.SocketTimeout)
        .setConnectTimeout(settingsCassandra.ConnectionTimeout)
        .setMaxConnsPerHost(settingsCassandra.MaxConnectionsPerHost)
        .setSeeds("localhost"))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
  }
*/

}