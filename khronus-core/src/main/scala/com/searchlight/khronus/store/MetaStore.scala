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

package com.searchlight.khronus.store

import java.lang
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.datastax.driver.core.{ BatchStatement, ResultSet, Session }
import com.searchlight.khronus.model.{ Metric, MonitoringSupport, Tick, Timestamp }
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

case class MetricMetadata(metric: Metric, timestamp: Timestamp)

trait MetaStore extends Snapshot[Map[Metric, (Timestamp, Boolean)]] {
  def update(metric: Seq[Metric], lastProcessedTimestamp: Timestamp, active: Boolean = true): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric, active: Boolean = true): Future[Unit]

  def allMetrics: Future[Seq[Metric]]

  def allActiveMetrics: Future[Seq[Metric]]

  def searchInSnapshotByRegex(regex: String): Seq[Metric]

  def searchInSnapshotByMetricName(metricName: String): Option[(Metric, (Timestamp, Boolean))]

  def notifyEmptySlice(metric: Metric, duration: Duration)

  def notifyMetricMeasurement(metric: Metric, active: Boolean)
}

trait MetaSupport {
  def metaStore: MetaStore = Meta.metaStore
}

class CassandraMetaStore(session: Session) extends MetaStore with Logging with CassandraUtils with ConcurrencySupport with MonitoringSupport with Measurable {
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

  override def initialValue = Map[Metric, (Timestamp, Boolean)]()

  override def getFreshData()(implicit executor: ExecutionContext): Future[Map[Metric, (Timestamp, Boolean)]] = {
    retrieveMetrics(executor)
  }

  def insert(metric: Metric, active: Boolean = true): Future[Unit] = {
    update(Seq(metric), (Tick().bucketNumber - 1).startTimestamp(), active)
  }

  def update(metrics: Seq[Metric], lastProcessedTimestamp: Timestamp, active: Boolean = true): Future[Unit] = executeChunked("meta", metrics, Settings.CassandraMeta.insertChunkSize) {
    metricsChunk ⇒
      val ts: lang.Long = Long.box(lastProcessedTimestamp.ms)
      val ac: lang.Boolean = lang.Boolean.valueOf(active)
      val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
      metricsChunk.foreach { metric ⇒
        val name: String = asString(metric)
        batchStmt.add(InsertStmt.bind(MetricsKey, name, ts, ac))
      }

      val future: Future[ResultSet] = session.executeAsync(batchStmt)
      future.map(_ ⇒ log.trace(s"Stored meta chunk successfully"))
  }

  def searchInSnapshotByRegex(regex: String): Seq[Metric] = {
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher("")
    snapshot.keys.filter(k ⇒ matcher.reset(k.name).matches()).toSeq
  }

  def searchInSnapshotByMetricName(metricName: String): Option[(Metric, (Timestamp, Boolean))] = {
    val pattern = Pattern.compile(metricName)
    val matcher = pattern.matcher("")
    snapshot.find { case (metric, (timestamp, active)) ⇒ matcher.reset(metric.name).matches() }
  }

  def allMetrics: Future[Seq[Metric]] = retrieveMetrics.map(_.keys.toSeq)

  def allActiveMetrics: Future[Seq[Metric]] = retrieveMetrics.map(_.filter { case (metric, (timestamp, active)) ⇒ active }.keys.toSeq)

  private def retrieveMetrics(implicit executor: ExecutionContext): Future[Map[Metric, (Timestamp, Boolean)]] = {
    log.debug("Retrieving meta...")
    val future: Future[ResultSet] = session.executeAsync(GetByKeyStmt.bind(MetricsKey).setFetchSize(10000))
    future.
      map(resultSet ⇒ {
        val metrics = resultSet.all().asScala.map { row ⇒
          (toMetric(row.getString("metric")), (Timestamp(row.getLong("timestamp")), row.getBool("active")))
        }.toMap
        log.debug(s"Found ${metrics.size} metrics in meta")
        metrics
      })(executor).
      andThen {
        case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
      }(executor)
  }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = {
    snapshot.get(metric) match {
      case Some((timestamp, active)) ⇒ Future.successful(timestamp)
      case None                      ⇒ getLastProcessedTimestampFromCassandra(metric)
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
    if (activeStatusBuffer.nonEmpty) {
      val keys = mutable.Buffer[String]()
      try {
        activeStatusBuffer.grouped(Settings.CassandraMeta.insertChunkSize).foreach(chunk ⇒ {
          val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
          chunk.foreach {
            case (metric, active) ⇒ {
              batchStmt.add(UpdateActiveStatus.bind(java.lang.Boolean.valueOf(active), MetricsKey, metric))
            }
          }
          session.execute(batchStmt)
          keys ++= chunk.keySet
        })

      } catch {
        case e: Throwable ⇒ log.error("Error changing meta active status", e)
      }

      keys foreach (activeStatusBuffer.remove(_))
    }

  private def deactivate(metric: Metric): Unit = {
    activeStatusBuffer.update(asString(metric), false)
    incrementCounter("metaStore.deactivate")
  }

  private def activate(metric: Metric): Unit = {
    activeStatusBuffer.update(asString(metric), true)
    incrementCounter("metaStore.activate")
  }

  def notifyMetricMeasurement(metric: Metric, active: Boolean) = {
    if (!active) {
      activate(metric)
    }
  }

  def notifyEmptySlice(metric: Metric, duration: Duration) = {
    if (Settings.Window.WindowDurations.last.equals(duration) && snapshot.get(metric).get._2) {
      deactivate(metric)
    }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def toMetric(key: String): Metric = {
    val tokens = key.split("\\|")
    if (tokens.length > 2) {
      Metric((key splitAt (key lastIndexOf '|'))._1, tokens.last)
    } else {
      Metric(tokens(0), tokens(1))
    }
  }

}