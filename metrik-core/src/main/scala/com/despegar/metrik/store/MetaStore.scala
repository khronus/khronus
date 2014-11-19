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
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import com.despegar.metrik.model.Metric
import com.despegar.metrik.model.Timestamp

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

  val columnFamily = ColumnFamily.newColumnFamily("meta", StringSerializer.get(), StringSerializer.get())
  val metricsKey = "metrics"

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

  def initialize = Cassandra.createColumnFamily(columnFamily)

  def insert(metric: Metric): Future[Unit] = {
    put(metric, Timestamp(1))
  }

  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit] = {
    put(metric, lastProcessedTimestamp)
  }

  private def put(metric: Metric, timestamp: Timestamp): Future[Unit] = Future {
    val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
    mutationBatch.withRow(columnFamily, metricsKey).putColumn(asString(metric), timestamp.ms)
    mutationBatch.execute()
    log.debug(s"$metric - Stored meta successfully. Timestamp: $timestamp")
  } andThen {
    case Failure(reason) ⇒ log.error(s"$metric - Failed to store meta", reason)
  }

  def searchInSnapshot(expression: String): Future[Seq[Metric]] = Future { getFromSnapshot.filter(_.name.matches(expression)) }

  def retrieveMetrics: Future[Seq[Metric]] = Future {
    val metrics = Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).execute().getResult.asScala.map(c ⇒ fromString(c.getName)).toSeq
    log.info(s"Found ${metrics.length} metrics in meta")
    metrics
  } andThen {
    case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
  }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = Future {
    Timestamp(Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).getColumn(asString(metric)).execute().getResult.getLongValue)
  } andThen {
    case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason)
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|")
    Metric(tokens(0), tokens(1))
  }

  override def getFreshData(): Future[Seq[Metric]] = {
    retrieveMetrics
  }

  override def context = asyncExecutionContext
}