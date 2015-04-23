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

import java.util.concurrent.{ ConcurrentLinkedQueue, TimeUnit }

import com.datastax.driver.core.{ BatchStatement, ResultSet, Session }
import com.despegar.khronus.model.{ Metric, Timestamp }
import com.despegar.khronus.util.log.Logging

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success }
import com.despegar.khronus.util.Settings

case class MetricMetadata(metric: Metric, timestamp: Timestamp)

trait MetaStore {
  def update(metric: Seq[Metric], lastProcessedTimestamp: Timestamp): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric): Future[Unit]

  def allMetrics: Future[Seq[Metric]]

  def searchInSnapshot(expression: String): Future[Seq[Metric]]

  def contains(metric: Metric): Boolean

  def getFromSnapshotSync(metricName: String): Option[(Metric, Timestamp)]
}

trait MetaSupport {
  def metaStore: MetaStore = Meta.metaStore
}

class CassandraMetaStore(session: Session) extends MetaStore with Logging with CassandraUtils with Snapshot[Map[Metric, Timestamp]] {
  //------- cassandra initialization
  val MetricsKey = "metrics"

  private val CreateTableStmt = s"create table if not exists meta (key text, metric text, timestamp bigint, primary key (key, metric));"
  retry(MaxRetries, "Creating table meta") {
    session.execute(CreateTableStmt)
  }
  private val InsertStmt = session.prepare(s"insert into meta (key, metric, timestamp) values (?, ?, ?);")
  private val GetByKeyStmt = session.prepare(s"select metric, timestamp from meta where key = ?;")
  private val GetLastProcessedTimeStmt = session.prepare(s"select timestamp from meta where key = ? and metric = ?;")

  implicit val asyncExecutionContext = executionContext("meta-worker")

  //------- snapshot conf
  override val snapshotName = "meta"

  override def initialValue = Map[Metric, Timestamp]()

  override def getFreshData()(implicit executor: ExecutionContext): Future[Map[Metric, Timestamp]] = {
    retrieveMetrics(executor)
  }

  def insert(metric: Metric): Future[Unit] = {
    update(Seq(metric), Timestamp(1))
  }

  def update(metrics: Seq[Metric], lastProcessedTimestamp: Timestamp): Future[Unit] = executeChunked("meta", metrics, Settings.CassandraMeta.insertChunkSize) {
    metricsChunk ⇒
      val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
      metricsChunk.foreach {
        metric ⇒ batchStmt.add(InsertStmt.bind(MetricsKey, asString(metric), Long.box(lastProcessedTimestamp.ms)))
      }

      val future: Future[ResultSet] = session.executeAsync(batchStmt)
      future.map(_ ⇒ log.trace(s"Stored meta chunk successfully"))
  }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future {
    getFromSnapshot.keys.filter(_.name.matches(expression)).toSeq
  }

  def contains(metric: Metric): Boolean = getFromSnapshot.contains(metric)

  def getFromSnapshotSync(metricName: String): Option[(Metric, Timestamp)] = {
    getFromSnapshot.find { case (metric, timestamp) ⇒ metric.name.matches(metricName) }
  }

  def allMetrics(): Future[Seq[Metric]] = retrieveMetrics.map(_.keys.toSeq)

  private def retrieveMetrics(implicit executor: ExecutionContext): Future[Map[Metric, Timestamp]] = {
    val future: Future[ResultSet] = session.executeAsync(GetByKeyStmt.bind(MetricsKey))
    future.
      map(resultSet ⇒ {
        val metrics = resultSet.all().asScala.map(row ⇒ (fromString(row.getString("metric")), Timestamp(row.getLong("timestamp")))).toMap
        log.info(s"Found ${metrics.size} metrics in meta")
        metrics
      })(executor).
      andThen {
        case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
      }(executor)
  }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = {
    getFromSnapshot.get(metric) match {
      case Some(timestamp) ⇒ Future.successful(timestamp)
      case None            ⇒ getLastProcessedTimestampFromCassandra(metric)
    }
  }

  def getLastProcessedTimestampFromCassandra(metric: Metric): Future[Timestamp] = {
    val future: Future[ResultSet] = session.executeAsync(GetLastProcessedTimeStmt.bind(MetricsKey, asString(metric)))
    future.
      map(resultSet ⇒ Timestamp(resultSet.one().getLong("timestamp"))).
      andThen {
        case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason)
      }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|")
    Metric(tokens(0), tokens(1))
  }

  //  override def context = asyncExecutionContext

}