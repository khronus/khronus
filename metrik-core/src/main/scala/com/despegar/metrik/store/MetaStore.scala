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

import java.util.concurrent.{ ConcurrentLinkedQueue, TimeUnit }

import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import scala.concurrent.Promise
import scala.util.{ Failure, Success }
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import com.despegar.metrik.model.Metric
import com.despegar.metrik.model.Timestamp
import com.despegar.metrik.util.log.Logging
import com.datastax.driver.core.BatchStatement

trait MetaStore extends Snapshot[Map[Metric, Timestamp]] {
  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric): Future[Unit]

  def allMetrics: Future[Seq[Metric]]

  def searchInSnapshot(expression: String): Future[Seq[Metric]]

  def contains(metric: Metric): Boolean

  def getMetricType(metricName: String): String
}

trait MetaSupport {
  def metaStore: MetaStore = CassandraMetaStore
}

object CassandraMetaStore extends MetaStore with Logging {

  val MetricsKey = "metrics"

  private lazy val CreateTableStmt = s"create table if not exists meta (key text, metric text, timestamp bigint, primary key (key, metric));"
  private lazy val InsertStmt = CassandraMeta.session.prepare(s"insert into meta (key, metric, timestamp) values (?, ?, ?);")
  private lazy val GetByKeyStmt = CassandraMeta.session.prepare(s"select metric from meta where key = ?;")
  private lazy val GetLastProcessedTimeStmt = CassandraMeta.session.prepare(s"select timestamp from meta where key = ? and metric = ?;")

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

  private val queue = new ConcurrentLinkedQueue[(Promise[Unit], (Metric, Timestamp))]()

  val asyncUpdateExecutor = Executors.newScheduledThreadPool(1)
  asyncUpdateExecutor.scheduleAtFixedRate(new Runnable() {
    override def run = updateAsync
  }, 1, 1, TimeUnit.SECONDS)


  import CassandraMeta._

  def initialize = CassandraMeta.session.execute(CreateTableStmt)

  def insert(metric: Metric): Future[Unit] = {
    put(Seq((metric, Timestamp(1))))
  }

  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit] = {
    val promise = Promise[Unit]()
    buffer(promise, (metric, lastProcessedTimestamp))
    promise.future
  }

  private def buffer(promise: Promise[Unit], tuple: (Metric, Timestamp)) = {
    queue.offer((promise, tuple))
  }

  private def updateAsync = {
    val elements = drain()
    if (!elements.isEmpty) {
      put(elements.map(element ⇒ element._2))
      elements.foreach(element ⇒ element._1.complete(Success(Unit)))
    }
  }

  @tailrec //TODO: refactorme
  private def drain(elements: Buffer[(Promise[Unit], (Metric, Timestamp))] = Buffer[(Promise[Unit], (Metric, Timestamp))]()): Buffer[(Promise[Unit], (Metric, Timestamp))] = {
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
        andThen { case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason) }


  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = {
    getFromSnapshot.get(metric) match {
      case Some(timestamp) ⇒ Future.successful(timestamp)
      case None            ⇒ getLastProcessedTimestampFromCassandra(metric)
    }
  }

  def getLastProcessedTimestampFromCassandra(metric: Metric): Future[Timestamp] = Future {
    Timestamp(Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).getColumn(asString(metric)).execute().getResult.getLongValue)
  } andThen {
    case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason)
  }

  private def put(metrics: Seq[(Metric, Timestamp)]): Future[Unit] = {

      val batchStmt = new BatchStatement();
      metrics.foreach { tuple ⇒
        val metric = tuple._1
        val timestamp = tuple._2
        batchStmt.add(InsertStmt.bind(MetricsKey, asString(metric), Long.box(timestamp.ms)))
      }

    Cassandra.session.executeAsync(batchStmt)
      .map(_ ⇒ log.debug(s"Stored meta (batch) successfully"))
      .andThen { case Failure(reason) ⇒ log.error("Failed to store meta", reason) }
  }


  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] =
    CassandraMeta.session.executeAsync(GetLastProcessedTimeStmt.bind(MetricsKey, asString(metric))).
      map(resultSet ⇒ Timestamp(resultSet.one().getLong("timestamp"))).
      andThen { case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason) }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future {
    getFromSnapshot.filter(_.name.matches(expression))
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|")
    Metric(tokens(0), tokens(1))
  }

  override def getFreshData(): Future[Map[Metric, Timestamp]] = {
    retrieveMetrics
  }

  def getMetricType(metricName: String): String = {
    val metric = getFromSnapshot.keys find (metric ⇒ metric.name.equalsIgnoreCase(metricName)) getOrElse (throw new UnsupportedOperationException(s"Metric not found: $metricName"))
    metric.mtype
  }

  override def context = asyncExecutionContext
}