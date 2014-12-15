/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.store

import com.datastax.driver.core.policies.{ TokenAwarePolicy, LoggingRetryPolicy, DefaultRetryPolicy, RoundRobinPolicy }
import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging
import com.datastax.driver.core._
import scala.concurrent.ExecutionContext
import com.google.common.util.concurrent.{ FutureCallback, Futures }

import scala.concurrent.{ Future, Promise }
import scala.util.{ Success, Failure, Try };

object CassandraCluster extends Logging with CassandraClusterConfiguration {
  private val cluster: Cluster = clusterBuilder.build()

  def connect() = cluster.connect()

  def close() = {
    log.info("Closing Cassandra cluster sessions")
    cluster.close()
  }

  sys.addShutdownHook(close)
}

trait CassandraClusterConfiguration {
  val settingsCassandra = Settings.CassandraCluster

  private val poolingOptions = new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, settingsCassandra.MaxConnectionsPerHost)
  private val socketOptions = new SocketOptions().setConnectTimeoutMillis(settingsCassandra.ConnectionTimeout).setReadTimeoutMillis(settingsCassandra.SocketTimeout)
  private val loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy)
  private val retryPolicy = new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE)

  def clusterBuilder = Cluster.builder().
    withClusterName(settingsCassandra.ClusterName).
    addContactPoints(settingsCassandra.Seeds: _*).
    withPort(settingsCassandra.Port).
    withPoolingOptions(poolingOptions).
    withSocketOptions(socketOptions).
    withLoadBalancingPolicy(loadBalancingPolicy).
    withRetryPolicy(retryPolicy)

}

trait CassandraSupport extends Logging {

  import scala.language.implicitConversions

  def keyspace: String

  val session: Session = connectCassandra
  val MaxRetries = 3

  def initialize(): Unit = {
    createSchemaIfNotExists
  }

  def getRF: Int

  def connectCassandra = CassandraCluster.connect()

  def createSchemaIfNotExists = {
    val keyspacePlusSuffix = keyspace + Settings.CassandraCluster.KeyspaceNameSuffix

    retry(MaxRetries, s"initialize schema $keyspacePlusSuffix") {
      log.info(s"Initializing schema: $keyspacePlusSuffix")
      session.execute(s"create keyspace if not exists $keyspacePlusSuffix with replication = {'class':'SimpleStrategy', 'replication_factor': $getRF};")
    }

    session.execute(s"USE $keyspacePlusSuffix;")
  }

  def truncate(table: String) = Try {
    session.execute(s"truncate $table;");
  }

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

  @annotation.tailrec
  final def retry(n: Int, action: String)(block: ⇒ Unit): Unit = {
    val result = Try { block }
    result match {
      case Success(_) ⇒
      case Failure(e) if n > 1 ⇒ {
        log.warn(s"Failed to $action - Retrying... ${e.getMessage}")
        retry(n - 1, action)(block)
      }
      case Failure(e) ⇒ {
        log.error(s"Failed to $action - No more retries", e)
        throw (e)
      }
    }
  }

}

object CassandraMeta extends CassandraMeta {
  initialize
}

trait CassandraMeta extends CassandraSupport {
  override def keyspace = "meta"

  override def initialize: Unit = {
    super.initialize

    retry(MaxRetries, "Creating table meta") { CassandraMetaStore.initialize }
  }

  override def getRF: Int = Settings.CassandraMeta.ReplicationFactor
}

object CassandraBuckets extends CassandraBuckets {
  initialize
}

trait CassandraBuckets extends CassandraSupport {
  override def keyspace = "buckets"

  override def initialize: Unit = {
    super.initialize
    retry(MaxRetries, "Creating bucket timer tables") { CassandraHistogramBucketStore.initialize }
    retry(MaxRetries, "Creating bucket counter tables") { CassandraCounterBucketStore.initialize }
  }

  override def getRF: Int = Settings.CassandraBuckets.ReplicationFactor
}

object CassandraSummaries extends CassandraSummaries {
  initialize
}

trait CassandraSummaries extends CassandraSupport {
  override def keyspace = "summaries"

  override def initialize: Unit = {
    super.initialize
    retry(MaxRetries, "Creating summary timer tables") { CassandraStatisticSummaryStore.initialize }
    retry(MaxRetries, "Creating summary counter tables") { CassandraCounterSummaryStore.initialize }
  }

  override def getRF: Int = Settings.CassandraSummaries.ReplicationFactor
}

case class Statements(insert: PreparedStatement, selects: Map[String, PreparedStatement], delete: Option[PreparedStatement])