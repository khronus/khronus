package com.despegar.metrik.store

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import com.despegar.metrik.model.{ Bucket, Metric }
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.ColumnListMutation
import com.netflix.astyanax.model.{ Column, ColumnFamily }
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure

trait BucketStoreSupport[T <: Bucket] {

  def bucketStore: BucketStore[T]
}

trait BucketStore[T <: Bucket] extends Logging {
  private val LIMIT = 1000
  private val INFINITE = 1L

  def windowDurations: Seq[Duration]

  lazy val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap

  def getColumnFamilyName(duration: Duration): String

  def toBucket(windowDuration: Duration)(column: Column[java.lang.Long]): T

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2))

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  def sliceUntil(metric: Metric, until: Long, sourceWindow: Duration): Future[Seq[T]] = {
    Future {
      executeSlice(metric, until, sourceWindow)
    } map { _.map { toBucket(sourceWindow) _ }.toSeq }
  }

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit] = {
    doUnit(buckets) {
      log.debug(s"${p(metric, windowDuration)} - Storing ${buckets.length} buckets ($buckets)")
      mutate(metric, windowDuration, buckets) { (mutation, bucket) ⇒
        mutation.putColumn(bucket.timestamp, serializeBucket(metric, windowDuration, bucket))
      }
    }
  }

  def remove(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit] = {
    doUnit(buckets) {
      log.debug(s"${p(metric, windowDuration)} - Removing ${buckets.length} buckets ($buckets)")
      mutate(metric, windowDuration, buckets) { (mutation, bucket) ⇒
        mutation.deleteColumn(bucket.timestamp)
      }
    }
  }

  def serializeBucket(metric: Metric, windowDuration: Duration, bucket: T): ByteBuffer

  private def executeSlice(metric: Metric, until: Long, windowDuration: Duration): Iterable[Column[java.lang.Long]] = {
    val result = Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(metric.name)
      .withColumnRange(INFINITE, until, false, LIMIT).execute().getResult().asScala

    log.debug(s"$metric - Found ${result.size} buckets of $windowDuration")
    result
  }

  private def doUnit(col: Seq[Any])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future {}
    }
  }

  private def mutate(metric: Metric, windowDuration: Duration, buckets: Seq[T])(f: (ColumnListMutation[java.lang.Long], T) ⇒ Unit) = {
    Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      val mutation = mutationBatch.withRow(columnFamilies(windowDuration), metric.name)
      buckets.foreach(f(mutation, _))
      mutationBatch.execute
      log.trace(s"$metric - Mutation successful")
    } andThen {
      case Failure(reason) ⇒ log.error(s"$metric - Mutation failed", reason)
    }
  }

}
