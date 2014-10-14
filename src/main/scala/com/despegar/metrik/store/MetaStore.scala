package com.despegar.metrik.store

import java.util.concurrent.Executors

import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait MetaStore {
  def store(metric: String): Future[Unit]
  def retrieveMetrics: Future[Seq[String]]
}

trait MetaSupport {
  def metaStore: MetaStore = CassandraMetaStore
}

object CassandraMetaStore extends MetaStore with Logging {

  val columnFamily = ColumnFamily.newColumnFamily("meta", StringSerializer.get(), StringSerializer.get())

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

  def initialize = Cassandra.createColumnFamily(columnFamily)

  def store(metric: String) = {
    val future = Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      mutationBatch.withRow(columnFamily, "metrics").putEmptyColumn(metric)
      mutationBatch.execute()
      log.info(s"Stored meta for $metric successfully")
    }
    future onFailure {
      case e: Exception ⇒ log.error(s"Failed to store meta for $metric", e)
    }
    future
  }

  def retrieveMetrics = {
    val future = Future {
      val metrics = Cassandra.keyspace.prepareQuery(columnFamily).getKey("metrics").execute().getResult().asScala.map(_.getName).toSeq
      log.info(s"Found ${metrics.length} metrics in meta")
      metrics
    }
    future.onFailure {
      case e: Exception ⇒ log.error(s"Failed to retrieve metrics from meta", e)
    }
    future
  }

}