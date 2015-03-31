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

package com.despegar.khronus.store

import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging
import com.google.common.util.concurrent.{ FutureCallback, Futures }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try };

object CassandraCluster extends Logging with CassandraClusterConfiguration {
  private val cluster: Cluster = clusterBuilder.build()

  /*
  val cassandraMeta = Meta
  val cassandraBuckets = Buckets
  val cassandraSummaries = Summaries
*/
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
    withReconnectionPolicy(new ExponentialReconnectionPolicy(1000l, 10 * 1000l)).
    withRetryPolicy(retryPolicy)

}

trait CassandraKeyspace extends Logging with CassandraUtils {

  import scala.language.implicitConversions

  def keyspace: String

  val session: Session = connectCassandra

  def initialize(): Unit = {
    val keyspacePlusSuffix = keyspace + Settings.CassandraCluster.KeyspaceNameSuffix

    log.info(s"Initializing schema: $keyspacePlusSuffix")
    retry(MaxRetries, s"initialize schema $keyspacePlusSuffix") {
      session.execute(s"create keyspace if not exists $keyspacePlusSuffix with replication = {'class':'SimpleStrategy', 'replication_factor': $getRF};")
    }

    session.execute(s"USE $keyspacePlusSuffix;")
  }

  def getRF: Int

  def connectCassandra = CassandraCluster.connect()

  def truncate(table: String) = Try {
    session.execute(s"truncate $table;");
  }

}

object Meta extends CassandraKeyspace {

  initialize
  val metaStore = new CassandraMetaStore(session)

  override def keyspace = "meta"

  override def getRF: Int = Settings.CassandraMeta.ReplicationFactor
}

object Buckets extends CassandraKeyspace {

  log.info("Will initialize Buckets....")
  initialize
  val histogramBucketStore = new CassandraHistogramBucketStore(session)
  val counterBucketStore = new CassandraCounterBucketStore(session)

  override def keyspace = "buckets"

  override def getRF: Int = Settings.CassandraBuckets.ReplicationFactor
}

object Summaries extends CassandraKeyspace {

  initialize
  val histogramSummaryStore = new CassandraStatisticSummaryStore(session)
  val counterSummaryStore = new CassandraCounterSummaryStore(session)

  override def keyspace = "summaries"

  override def getRF: Int = Settings.CassandraSummaries.ReplicationFactor
}

case class Statements(insert: PreparedStatement, selects: Map[String, PreparedStatement], delete: Option[PreparedStatement])

trait CassandraUtils extends Logging {
  val MaxRetries = 3

  implicit def resultSetFutureUnitToScala(f: ResultSetFuture): Future[Unit] = {
    val p = Promise[Unit]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success Unit

        def onFailure(t: Throwable) = p failure t
      })
    p.future
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
    val result = Try {
      block
    }
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

  final def executeChunked[T](msg: String, items: Seq[T], chunkSize: Int)(block: Seq[T] ⇒ Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    if (!items.isEmpty) {
      val futures = items.grouped(chunkSize).map(chunk ⇒ block(chunk))
      Future.sequence(futures).andThen {
        case Failure(reason) ⇒ log.error(s"Failed to execute chunk operation: $msg", reason)
        case Success(_)      ⇒ log.info(s"Chunk operation finished ok: $msg")
      }.map(_ ⇒ ())
    } else {
      Future.successful(())
    }
  }

}