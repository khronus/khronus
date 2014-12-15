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

import java.util.concurrent.{ ConcurrentLinkedQueue, TimeUnit }

import com.datastax.driver.core.BatchStatement
import com.despegar.khronus.model.{ Metric, Timestamp }
import com.despegar.khronus.util.log.Logging

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success }

case class MetricMetadata(metric: Metric, timestamp: Timestamp)

trait MetaStore extends Snapshot[Map[Metric, Timestamp]] {
  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric): Future[Unit]

  def allMetrics: Future[Seq[Metric]]

  def searchInSnapshot(expression: String): Future[Seq[Metric]]

  def contains(metric: Metric): Boolean
}

trait MetaSupport {
  def metaStore: MetaStore = CassandraMetaStore
}

object CassandraMetaStore extends MetaStore with Logging {

  val MetricsKey = "metrics"

  private val CreateTableStmt = s"create table if not exists meta (key text, metric text, timestamp bigint, primary key (key, metric));"
  private val InsertStmt = CassandraMeta.session.prepare(s"insert into meta (key, metric, timestamp) values (?, ?, ?);")
  private val GetByKeyStmt = CassandraMeta.session.prepare(s"select metric, timestamp from meta where key = ?;")
  private val GetLastProcessedTimeStmt = CassandraMeta.session.prepare(s"select timestamp from meta where key = ? and metric = ?;")

  implicit val asyncExecutionContext = executionContext("meta-worker", 5)

  private val queue = new ConcurrentLinkedQueue[(Promise[Unit], MetricMetadata)]()

  val asyncUpdateExecutor = scheduledThreadPool("meta-writer-worker")

  asyncUpdateExecutor.scheduleAtFixedRate(new Runnable() {
    override def run = updateAsync
  }, 1, 1, TimeUnit.SECONDS)

  import com.despegar.khronus.store.CassandraMeta._

  def initialize = CassandraMeta.session.execute(CreateTableStmt)

  def insert(metric: Metric): Future[Unit] = {
    put(Seq(MetricMetadata(metric, Timestamp(1))))
  }

  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit] = {
    val promise = Promise[Unit]()
    buffer(promise, MetricMetadata(metric, lastProcessedTimestamp))
    promise.future
  }

  private def buffer(promise: Promise[Unit], metricMetadata: MetricMetadata) = {
    queue.offer((promise, metricMetadata))
  }

  private def updateAsync = {
    val elements = drain()
    if (!elements.isEmpty) {
      put(elements.map(element ⇒ element._2))
      elements.foreach(element ⇒ element._1.complete(Success(Unit)))
    }
  }

  @tailrec //TODO: refactorme
  private def drain(elements: Buffer[(Promise[Unit], MetricMetadata)] = Buffer[(Promise[Unit], MetricMetadata)]()): Buffer[(Promise[Unit], MetricMetadata)] = {
    val element = queue.poll()
    if (element != null) {
      elements += element
      drain(elements)
    } else {
      elements
    }
  }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future {
    getFromSnapshot.keys.filter(_.name.matches(expression)).toSeq
  }

  def contains(metric: Metric): Boolean = getFromSnapshot.contains(metric)

  def allMetrics(): Future[Seq[Metric]] = retrieveMetrics.map(_.keys.toSeq)

  def retrieveMetrics: Future[Map[Metric, Timestamp]] = CassandraMeta.session.executeAsync(GetByKeyStmt.bind(MetricsKey)).
    map(resultSet ⇒ {
      val metrics = resultSet.all().asScala.map(row ⇒ (fromString(row.getString("metric")), Timestamp(row.getLong("timestamp")))).toMap
      log.info(s"Found ${metrics.size} metrics in meta")
      metrics
    }).
    andThen {
      case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
    }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = {
    getFromSnapshot.get(metric) match {
      case Some(timestamp) ⇒ Future.successful(timestamp)
      case None            ⇒ getLastProcessedTimestampFromCassandra(metric)
    }
  }

  def getLastProcessedTimestampFromCassandra(metric: Metric): Future[Timestamp] =
    CassandraMeta.session.executeAsync(GetLastProcessedTimeStmt.bind(MetricsKey, asString(metric))).
      map(resultSet ⇒ Timestamp(resultSet.one().getLong("timestamp"))).
      andThen {
        case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason)
      }

  private def put(metrics: Seq[MetricMetadata]): Future[Unit] = {
    val batchStmt = new BatchStatement();
    metrics.foreach {
      metricMetadata ⇒ batchStmt.add(InsertStmt.bind(MetricsKey, asString(metricMetadata.metric), Long.box(metricMetadata.timestamp.ms)))
    }

    CassandraMeta.session.executeAsync(batchStmt)
      .map(_ ⇒ log.debug(s"Stored meta (batch) successfully"))
      .andThen {
        case Failure(reason) ⇒ log.error("Failed to store meta", reason)
      }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|")
    Metric(tokens(0), tokens(1))
  }

  override def getFreshData(): Future[Map[Metric, Timestamp]] = {
    retrieveMetrics
  }

  override def context = asyncExecutionContext

  override val snapshotName = "meta"

  override def initialValue = Map[Metric, Timestamp]()
}