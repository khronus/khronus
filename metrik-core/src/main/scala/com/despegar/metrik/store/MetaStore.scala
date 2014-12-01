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

import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import com.despegar.metrik.model.Metric
import com.despegar.metrik.model.Timestamp
import com.despegar.metrik.util.log.Logging
import com.despegar.metrik.util.Settings
import com.datastax.driver.core.querybuilder.QueryBuilder

trait MetaStore extends Snapshot[Seq[Metric]] {
  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit]

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp]

  def insert(metric: Metric): Future[Unit]

  def retrieveMetrics: Future[Seq[Metric]]

  def searchInSnapshot(expression: String): Future[Seq[Metric]]

  def getMetricType(metricName: String): String = {
    val metric = getFromSnapshot find (metric ⇒ metric.name.equalsIgnoreCase(metricName)) getOrElse (throw new UnsupportedOperationException(s"Metric not found: $metricName"))
    metric.mtype
  }
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

  import CassandraMeta._

  def initialize = CassandraMeta.session.execute(CreateTableStmt)

  def insert(metric: Metric): Future[Unit] = {
    put(metric, Timestamp(1))
  }

  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit] = {
    put(metric, lastProcessedTimestamp)
  }

  private def put(metric: Metric, timestamp: Timestamp): Future[Unit] = {
    val preparedStmt = InsertStmt.bind(MetricsKey, asString(metric), Long.box(timestamp.ms))

    CassandraMeta.session.executeAsync(preparedStmt)
      .map(_ ⇒ log.debug(s"$metric - Stored meta successfully. Timestamp: $timestamp"))
      .andThen { case Failure(reason) ⇒ log.error(s"$metric - Failed to store meta", reason) }
  }

  def retrieveMetrics: Future[Seq[Metric]] = CassandraMeta.session.executeAsync(GetByKeyStmt.bind(MetricsKey)).
    map(resultSet ⇒ {
      val metrics = resultSet.all().asScala.map(row ⇒ fromString(row.getString("metric")))
      log.info(s"Found ${metrics.length} metrics in meta")
      metrics
    }).
    andThen { case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason) }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] =
    CassandraMeta.session.executeAsync(GetLastProcessedTimeStmt.bind(MetricsKey, asString(metric))).
      map(resultSet ⇒ Timestamp(resultSet.one().getLong("timestamp"))).
      andThen { case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason) }

  override def getFreshData(): Future[Seq[Metric]] = {
    retrieveMetrics
  }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future {
    getFromSnapshot.filter(_.name.matches(expression))
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|")
    Metric(tokens(0), tokens(1))
  }

  override def context = asyncExecutionContext
}