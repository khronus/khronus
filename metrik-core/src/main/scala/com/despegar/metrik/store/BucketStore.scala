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
import com.despegar.metrik.model.{ UniqueTimestamp, Bucket, Metric, Timestamp }
import com.despegar.metrik.util.{ Measurable, Logging }
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

trait BucketStore[T <: Bucket] extends Logging with Measurable {
  private val LIMIT = 30000

  def windowDurations: Seq[Duration]

  lazy val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), UniqueTimestamp.serializer))).toMap

  def getColumnFamilyName(duration: Duration): String

  def toBucket(windowDuration: Duration)(column: Column[UniqueTimestamp]): T

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2, Map("comparator_type" -> "CompositeType(LongType, TimeUUIDType)")))

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  def slice(metric: Metric, from: Timestamp, to: Timestamp, sourceWindow: Duration): Future[Seq[(UniqueTimestamp, () ⇒ T)]] = {
    Future {
      executeSlice(metric, from, to, sourceWindow)
    } map { _.map { column ⇒ (column.getName, () ⇒ toBucket(sourceWindow)(column)) }.toSeq }
  }

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit] = {
    ifNotEmpty(buckets) {
      log.debug(s"${p(metric, windowDuration)} - Storing ${buckets.length} buckets")
      mutate(metric, windowDuration, buckets) { (mutation, bucket) ⇒
        val serializedBucket: ByteBuffer = serializeBucket(metric, windowDuration, bucket)
        log.info(s"${p(metric, windowDuration)} Storing a bucket of ${serializedBucket.limit()} bytes")
        mutation.putColumn(UniqueTimestamp(bucket.timestamp), serializedBucket, ttl(windowDuration))
      }
    }
  }

  def remove(metric: Metric, windowDuration: Duration, bucketTimestamps: Seq[UniqueTimestamp]): Future[Unit] = {
    ifNotEmpty(bucketTimestamps) {
      log.debug(s"${p(metric, windowDuration)} - Removing ${bucketTimestamps.length} buckets")
      mutate(metric, windowDuration, bucketTimestamps) { (mutation, uniqueTimestamp) ⇒
        mutation.deleteColumn(uniqueTimestamp)
      }
    }
  }

  def serializeBucket(metric: Metric, windowDuration: Duration, bucket: T): ByteBuffer

  private def executeSlice(metric: Metric, from: Timestamp, to: Timestamp, windowDuration: Duration): Iterable[Column[UniqueTimestamp]] = measureTime("Slice", metric, windowDuration) {
    val result = Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(metric.name)
      .withColumnRange(UniqueTimestamp.serializer.buildRange().
        greaterThanEquals(from.ms).
        lessThanEquals(to.ms).limit(LIMIT)).execute().getResult().asScala

    log.debug(s"${p(metric, windowDuration)} Found ${result.size} buckets slicing from ${date(from.ms)} to ${date(to.ms)}")
    result
  }

  private def ifNotEmpty(col: Seq[Any])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future.successful(())
    }
  }

  private def mutate[T](metric: Metric, windowDuration: Duration, buckets: Seq[T])(f: (ColumnListMutation[UniqueTimestamp], T) ⇒ Unit) = {
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

  protected def ttl(windowDuration: Duration): Int

}
