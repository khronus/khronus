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

import java.nio.ByteBuffer
import java.util.concurrent.Executors
import com.despegar.metrik.model.{ Bucket, HistogramBucket }
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.ColumnListMutation
import com.netflix.astyanax.model.{ Column, ColumnFamily }
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }
import org.HdrHistogram.Histogram
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import com.despegar.metrik.model.Metric
import scala.util.Failure

trait HistogramBucketSupport extends BucketStoreSupport[HistogramBucket] {
  override def bucketStore: BucketStore[HistogramBucket] = CassandraHistogramBucketStore
}

object CassandraHistogramBucketStore extends BucketStore[HistogramBucket] with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(1 millis, 30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour) //FIXME put configured windows
  val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap

  val LIMIT = 1000

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2))

  def sliceUntil(metric: Metric, until: Long, sourceWindow: Duration): Future[Seq[HistogramBucket]] = {
    Future {
      executeSlice(metric, until, sourceWindow)
    } map { _.map { toHistogramBucketOf(sourceWindow) _ }.toSeq }
  }

  private def executeSlice(metric: Metric, until: Long, windowDuration: Duration): Iterable[Column[java.lang.Long]] = {
    log.debug(s"Slicing window of $windowDuration for metric $metric")
    val result = Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(getKey(metric, windowDuration))
      .withColumnRange(infinite, until, false, LIMIT).execute().getResult().asScala
    log.debug(s"Slicing window. Found ${result.size} buckets of $windowDuration for metric $metric")
    result
  }

  private def toHistogramBucketOf(windowDuration: Duration)(column: Column[java.lang.Long]) = {
    val timestamp = column.getName()
    val histogram = deserializeHistogram(column.getByteBufferValue)
    new HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
  }

  def store(metric: Metric, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]): Future[Unit] = {
    doUnit(histogramBuckets) {
      log.debug(s"Storing ${histogramBuckets.length} histogram buckets for metric $metric in window $windowDuration: $histogramBuckets")
      mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) ⇒
        mutation.putColumn(bucket.timestamp, serializeHistogram(bucket.asInstanceOf[HistogramBucket].histogram)) //FIXME
      }
    }
  }

  def remove(metric: Metric, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    doUnit(histogramBuckets) {
      log.debug(s"Removing ${histogramBuckets.length} histogram buckets for metric $metric in window $windowDuration")
      mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) ⇒
        mutation.deleteColumn(bucket.timestamp)
      }
    }
  }

  def doUnit(col: Seq[Any])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future {}
    }
  }

  private def mutate(metric: Metric, windowDuration: Duration, histogramBuckets: Seq[Bucket])(f: (ColumnListMutation[java.lang.Long], Bucket) ⇒ Unit) = {
    Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      val mutation = mutationBatch.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
      histogramBuckets.foreach(f(mutation, _))
      mutationBatch.execute
      log.trace("Mutation successful")
    } andThen {
      case Failure(reason) ⇒ log.error("Mutation failed", reason)
    }
  }

  private def getColumnFamilyName(duration: Duration) = s"bucket${duration.length}${duration.unit}"

  private def getKey(metric: Metric, windowDuration: Duration): String = metric.name

  private def serializeHistogram(histogram: Histogram): ByteBuffer = {
    val buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = histogram.encodeIntoCompressedByteBuffer(buffer) //TODO: Find a better way to do this serialization
    log.debug(s"Histogram with ${histogram.getTotalCount()} measures encoded and compressed into $bytesEncoded bytes")
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)

  private def infinite = 1L

}