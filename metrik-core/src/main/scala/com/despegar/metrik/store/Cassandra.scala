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

import com.datastax.driver.core.policies.{ TokenAwarePolicy, LoggingRetryPolicy, DefaultRetryPolicy, RoundRobinPolicy }
import com.despegar.metrik.util.Settings
import com.despegar.metrik.util.log.Logging
import com.datastax.driver.core._
import scala.concurrent.ExecutionContext
import com.google.common.util.concurrent.{ FutureCallback, Futures }

import scala.concurrent.{ Future, Promise }
import scala.util.Try;

object CassandraCluster extends Logging {
  lazy val settingsCassandra = Settings().CassandraCluster

  private lazy val poolingOptions = new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, settingsCassandra.MaxConnectionsPerHost)
  private lazy val socketOptions = new SocketOptions().setConnectTimeoutMillis(settingsCassandra.ConnectionTimeout).setReadTimeoutMillis(settingsCassandra.SocketTimeout)
  private lazy val loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy)
  private lazy val retryPolicy = new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE)

  private lazy val cluster: Cluster = Cluster.builder().
    withClusterName(settingsCassandra.ClusterName).
    addContactPoints(settingsCassandra.Seeds: _*).
    withPort(settingsCassandra.Port).
    withPoolingOptions(poolingOptions).
    withSocketOptions(socketOptions).
    withLoadBalancingPolicy(loadBalancingPolicy).
    withRetryPolicy(retryPolicy).
    build();

  def connect() = cluster.connect()

  def close() = cluster.close()

  sys.addShutdownHook(close)

}

abstract class CassandraSupport(keyspace: String) extends Logging {

  lazy val session: Session = CassandraCluster.connect()

  def initialize: Unit = {
    createSchemaIfNotExists
  }

  def getRF: Int

  def createSchemaIfNotExists = {
    log.info(s"Initializing schema: $keyspace")
    session.execute(s"create keyspace if not exists $keyspace with replication = {'class':'SimpleStrategy', 'replication_factor': $getRF};")
    session.execute(s"USE $keyspace;")
  }

  def close: Unit = {
    log.info(s"Closing cassandra session for keyspace $keyspace")
    session.close()
  }

  def truncate(table: String) = Try {
    session.execute(s"truncate $table;");
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

  def toFutureUnit(future: Future[ResultSet])(implicit executionContext: ExecutionContext): Future[Unit] = {
    future.map {
      case _ ⇒ ()
    }
  }
}

object CassandraMeta extends CassandraSupport("meta") {

  override def initialize: Unit = {
    super.initialize
    CassandraMetaStore.initialize
  }

  override def getRF: Int = Settings().CassandraMeta.ReplicationFactor
}

object CassandraBuckets extends CassandraSupport("buckets") {

  override def initialize: Unit = {
    super.initialize
    CassandraHistogramBucketStore.initialize
    CassandraCounterBucketStore.initialize
  }

  override def getRF: Int = Settings().CassandraBuckets.ReplicationFactor
}

object CassandraSummaries extends CassandraSupport("summaries") {

  override def initialize: Unit = {
    super.initialize
    CassandraStatisticSummaryStore.initialize
    CassandraCounterSummaryStore.initialize
  }

  override def getRF: Int = Settings().CassandraSummaries.ReplicationFactor
}

case class Statements(insert: PreparedStatement, selects: Map[String, PreparedStatement], delete: Option[PreparedStatement])