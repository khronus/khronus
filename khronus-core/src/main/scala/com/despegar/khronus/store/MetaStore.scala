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

import java.util.concurrent.TimeUnit

import com.datastax.driver.core.{ BatchStatement, ResultSet, Session }
import com.despegar.khronus.model.{ Tick, Metric, MonitoringSupport, Timestamp }
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ Measurable, ConcurrencySupport, Settings }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

case class MetricMetadata(metric: Metric, timestamp: Timestamp)

trait MetaStore {
  def update(metric: Seq[Metric], lastProcessedTimestamp: Timestamp): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric): Future[Unit]

  def allMetrics: Future[Seq[Metric]]

  def allActiveMetrics: Future[Seq[Metric]]

  def searchInSnapshot(expression: String): Future[Seq[Metric]]

  def searchInSnapshot(metricName: String, metricType: String): Option[Metric]

  def getFromSnapshotSync(metricName: String): Option[(Metric, Timestamp)]

  def notifyEmptySlice(metric: Metric, duration: Duration)

  def notifyMetricMeasurement(metric: Metric)
}

trait MetaSupport {
  def metaStore: MetaStore = Meta.metaStore
}

class CassandraMetaStore(session: Session) extends MetaStore with Logging with CassandraUtils with Snapshot[Map[Metric, Timestamp]] with ConcurrencySupport with MonitoringSupport with Measurable {
  //------- cassandra initialization
  val MetricsKey = "metrics"

  private val CreateTableStmt = s"create table if not exists meta (key text, metric text, timestamp bigint, active boolean, primary key (key, metric));"
  retry(MaxRetries, "Creating table meta") {
    session.execute(CreateTableStmt)
  }
  private val InsertStmt = session.prepare(s"insert into meta (key, metric, timestamp, active) values (?, ?, ?, ?);")
  private val GetByKeyStmt = session.prepare(s"select metric, timestamp, active from meta where key = ?;")
  private val GetLastProcessedTimeStmt = session.prepare(s"select timestamp from meta where key = ? and metric = ?;")
  private val UpdateActiveStatus = session.prepare(s"update meta set active = ? where key = ? and metric = ?;")

  //for buffering active status updates
  private var activeStatusBuffer = TrieMap.empty[String, Boolean]
  val scheduler = scheduledThreadPool("meta-flusher-worker")
  scheduler.scheduleAtFixedRate(new Runnable() {
    override def run() = changeActiveStatus()
  }, 0, 2, TimeUnit.SECONDS)

  implicit val asyncExecutionContext = executionContext("meta-worker")

  //------- snapshot conf
  override val snapshotName = "meta"

  override def initialValue = Map[Metric, Timestamp]()

  override def getFreshData()(implicit executor: ExecutionContext): Future[Map[Metric, Timestamp]] = {
    retrieveMetrics(executor)
  }

  def insert(metric: Metric): Future[Unit] = {
    update(Seq(metric), (Tick().bucketNumber - 1).startTimestamp())
  }

  def update(metrics: Seq[Metric], lastProcessedTimestamp: Timestamp): Future[Unit] = executeChunked("meta", metrics, Settings.CassandraMeta.insertChunkSize) {
    metricsChunk ⇒
      val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
      metricsChunk.foreach {
        metric ⇒ batchStmt.add(InsertStmt.bind(MetricsKey, asString(metric), Long.box(lastProcessedTimestamp.ms), new java.lang.Boolean(metric.active)))
      }

      val future: Future[ResultSet] = session.executeAsync(batchStmt)
      future.map(_ ⇒ log.trace(s"Stored meta chunk successfully"))
  }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future {
    getFromSnapshot.keys.filter(_.name.matches(expression)).toSeq
  }

  def searchInSnapshot(metricName: String, metricType: String): Option[Metric] = measureTime("metaStore.searchInSnapshot", "") {
    getFromSnapshot.keys.find(e ⇒ e.name.equals(metricName) && e.mtype.equals(metricType))
  }

  def getFromSnapshotSync(metricName: String): Option[(Metric, Timestamp)] = {
    getFromSnapshot.find { case (metric, timestamp) ⇒ metric.name.matches(metricName) }
  }

  def allMetrics(): Future[Seq[Metric]] = retrieveMetrics.map(_.keys.toSeq)

  def allActiveMetrics(): Future[Seq[Metric]] = retrieveMetrics.map(_.keys.filter(_.active).toSeq)

  private def retrieveMetrics(implicit executor: ExecutionContext): Future[Map[Metric, Timestamp]] = {
    val future: Future[ResultSet] = session.executeAsync(GetByKeyStmt.bind(MetricsKey))
    future.
      map(resultSet ⇒ {
        val metrics = resultSet.all().asScala.map(row ⇒ (toMetric(row.getString("metric"), row.getBool("active")), Timestamp(row.getLong("timestamp")))).toMap
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

  private def changeActiveStatus(): Unit =
    if (!activeStatusBuffer.isEmpty) {
      try {
        val older = activeStatusBuffer
        activeStatusBuffer = TrieMap.empty[String, Boolean]

        older.grouped(Settings.CassandraMeta.insertChunkSize).foreach(chunk ⇒ {
          val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
          chunk.foreach {
            case (metric, active) ⇒ batchStmt.add(UpdateActiveStatus.bind(new java.lang.Boolean(active), MetricsKey, metric))
          }

          session.execute(batchStmt)
        })
      } catch {
        case e: Throwable ⇒ log.error("Error changing meta active status", e)
      }
    }

  private def deactivate(metric: Metric): Unit = {
    activeStatusBuffer.update(asString(metric), false)
    incrementCounter("metaStore.deactivate")
  }

  private def activate(metric: Metric): Unit = measureTime("metaStore.activate", "") {
    activeStatusBuffer.update(asString(metric), true)
  }

  def notifyMetricMeasurement(metric: Metric) = {
    if (!metric.active) {
      activate(metric)
    }
  }

  def notifyEmptySlice(metric: Metric, duration: Duration) = {
    if (Settings.Window.WindowDurations.last.equals(duration) && metric.active) {
      deactivate(metric)
    }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def toMetric(key: String, active: Boolean): Metric = {
    val tokens = key.split("\\|")
    if (tokens.length > 2) {
      Metric((key splitAt (key lastIndexOf '|'))._1, tokens.last, active)
    } else {
      Metric(tokens(0), tokens(1), active)
    }
  }

  //  override def context = asyncExecutionContext

}