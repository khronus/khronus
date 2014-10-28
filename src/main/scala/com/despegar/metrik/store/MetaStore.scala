package com.despegar.metrik.store

import java.util.concurrent.Executors
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import com.despegar.metrik.model.Metric

trait MetaStore {
  def update(metric: Metric, lastProcessedTimestamp: Long): Future[Unit]
  def getLastProcessedTimestamp(metric: Metric): Future[Long]
  def insert(metric: Metric): Future[Unit]
  def retrieveMetrics: Future[Seq[Metric]]
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
    put(metric, -Long.MaxValue)
  }

  def update(metric: Metric, lastProcessedTimestamp: Long): Future[Unit] = {
    put(metric, lastProcessedTimestamp)
  }

  private def put(metric: Metric, timestamp: Long): Future[Unit] = {
    Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      mutationBatch.withRow(columnFamily, metricsKey).putColumn(asString(metric), timestamp)
      mutationBatch.execute()
      log.info(s"Stored meta for $metric successfully. Timestamp: $timestamp")
    } andThen {
      case Failure(reason) ⇒ log.error(s"Failed to store meta for $metric", reason)
    }
  }

  def retrieveMetrics = {
    Future {
      val metrics = Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).execute().getResult().asScala.map(c ⇒ fromString(c.getName)).toSeq
      log.info(s"Found ${metrics.length} metrics in meta")s
      metrics
    } andThen {
      case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
    }
  }

  def getLastProcessedTimestamp(metric: Metric): Future[Long] = {
    Future {
      Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).getColumn(asString(metric)).execute().getResult.getLongValue
    } andThen {
      case Failure(reason) ⇒ log.error(s"Failed to retrieve last processed timestamp of $metric from meta", reason)
    }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|");
    Metric(tokens(0), tokens(1))
  }

}