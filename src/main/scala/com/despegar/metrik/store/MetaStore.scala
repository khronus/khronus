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
    put(metric, Timestamp(-Long.MaxValue))
  }

  def update(metric: Metric, lastProcessedTimestamp: Timestamp): Future[Unit] = {
    put(metric, lastProcessedTimestamp)
  }

  private def put(metric: Metric, timestamp: Timestamp): Future[Unit] = {
    Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      mutationBatch.withRow(columnFamily, metricsKey).putColumn(asString(metric), timestamp.ms)
      mutationBatch.execute()
      log.debug(s"$metric - Stored meta successfully. Timestamp: $timestamp")
    } andThen {
      case Failure(reason) ⇒ log.error(s"$metric - Failed to store meta", reason)
    }
  }

  def retrieveMetrics = {
    Future {
      val metrics = Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).execute().getResult().asScala.map(c ⇒ fromString(c.getName)).toSeq
      log.info(s"Found ${metrics.length} metrics in meta")
      metrics
    } andThen {
      case Failure(reason) ⇒ log.error(s"Failed to retrieve metrics from meta", reason)
    }
  }

  def getLastProcessedTimestamp(metric: Metric): Future[Timestamp] = {
    Future {
      Timestamp(Cassandra.keyspace.prepareQuery(columnFamily).getKey(metricsKey).getColumn(asString(metric)).execute().getResult.getLongValue)
    } andThen {
      case Failure(reason) ⇒ log.error(s"$metric - Failed to retrieve last processed timestamp from meta", reason)
    }
  }

  private def asString(metric: Metric) = s"${metric.name}|${metric.mtype}"

  private def fromString(str: String): Metric = {
    val tokens = str.split("\\|");
    Metric(tokens(0), tokens(1))
  }

  override def getFreshData(): Future[Seq[Metric]] = {
    retrieveMetrics
  }

  override def context = asyncExecutionContext
}