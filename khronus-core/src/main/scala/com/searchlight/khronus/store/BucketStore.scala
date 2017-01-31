/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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
 * ========================================== ===============================================
 */

package com.searchlight.khronus.store

import java.nio.ByteBuffer

import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core._
import com.google.common.collect.{ Lists, Sets }
import com.searchlight.khronus.model._
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

trait BucketStoreSupport[T <: Bucket] {

  def bucketStore: BucketStore[T]
}

trait BucketStore[T <: Bucket] {
  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit]

  def store(metrics: Seq[(Metric, () ⇒ T)], windowDuration: Duration): Future[Unit]

  def slice(metric: Metric, from: Timestamp, to: Timestamp, sourceWindow: Duration): Future[BucketSlice[T]]
}

abstract class CassandraBucketStore[T <: Bucket](session: Session) extends BucketStore[T] with Logging with Measurable with ConcurrencySupport with CassandraUtils {

  import CassandraBucketStore.asyncExecutionContext

  protected def tableName(duration: Duration): String

  protected def windowDurations: Seq[Duration] = Settings.Window.WindowDurations

  protected def ttl(windowDuration: Duration): Int

  protected def limit: Int

  protected def fetchSize: Int

  protected def deserialize(windowDuration: Duration, timestamp: Long, bytes: Array[Byte]): T

  protected def serialize(metric: Metric, windowDuration: Duration, bucket: T): ByteBuffer

  val SliceQuery = "sliceQuery"

  windowDurations.foreach(window ⇒ {
    log.info(s"Initializing table ${tableName(window)}")
    retry(MaxRetries, s"Creating ${tableName(window)} table") {
      session.execute(s"create table if not exists ${tableName(window)} (metric text, timestamp bigint, buckets ${getBucketsCollectionType(window)}<blob>, primary key (metric, timestamp)) with gc_grace_seconds = 0 and compaction = {'class': 'LeveledCompactionStrategy' };")
    }
  })

  val stmtPerWindow: Map[Duration, Statements] = windowDurations.map(windowDuration ⇒ {
    log.info("creating prepared statements...")
    val insert = session.prepare(s"update ${tableName(windowDuration)} using ttl ${ttl(windowDuration)} set buckets = buckets + ? where metric = ? and timestamp = ? ; ")

    val simpleStmt = new SimpleStatement(s"select timestamp, buckets from ${tableName(windowDuration)} where metric = ? and timestamp >= ? and timestamp < ? limit ?;")
    simpleStmt.setFetchSize(fetchSize)
    val select = session.prepare(simpleStmt)

    val delete = session.prepare(s"delete from ${tableName(windowDuration)} where metric = ? and timestamp = ?;")
    (windowDuration, Statements(insert, Map(SliceQuery -> select), Some(delete)))
  }).toMap

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit] = executeChunked(s"bucket of $metric-$windowDuration", buckets, Settings.CassandraBuckets.insertChunkSize) {
    bucketsChunk ⇒
      {
        log.trace(s"${p(metric, windowDuration)} - Storing chunk of ${bucketsChunk.length} buckets")

        val boundBatchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
        val stmt = stmtPerWindow(windowDuration).insert
        bucketsChunk.foreach(bucket ⇒ {
          val serializedBucket = serialize(metric, windowDuration, bucket)
          log.trace(s"${p(metric, windowDuration)} Storing a bucket of ${serializedBucket.limit()} bytes")
          boundBatchStmt.add(stmt.bind(bucketCollection(serializedBucket, windowDuration), metric.name, Long.box(bucket.timestamp.ms)))
        })

        val future: Future[Unit] = session.executeAsync(boundBatchStmt)

        future
      }
  }

  def store(metrics: Seq[(Metric, () ⇒ T)], windowDuration: Duration): Future[Unit] = executeChunked(s"buckets of $windowDuration", metrics, Settings.CassandraBuckets.insertChunkSize) {
    bucketsChunk ⇒
      {
        val stmt = stmtPerWindow(windowDuration).insert
        val bounds = bucketsChunk.map {
          case (metric, fBucket) ⇒ {
            val bucket = fBucket()
            val serializedBucket = serialize(metric, windowDuration, bucket)
            val f: Future[Unit] = session.executeAsync(stmt.bind(Seq(serializedBucket).asJava, metric.name, Long.box(bucket.timestamp.ms)))
            f
          }
        }

        val future = Future.sequence(bounds)

        future andThen {
          case Success(_) ⇒ incrementCounter("bucketStore.batch.ok")
          case Failure(ex) ⇒ {
            log.error("Fail to execute batch store of posted measures", ex)
            incrementCounter("bucketStore.batch.fail")
          }
        }

        future.map(s ⇒ Unit)
      }
  }

  def slice(metric: Metric, from: Timestamp, to: Timestamp, sourceWindow: Duration): Future[BucketSlice[T]] = measureFutureTime("slice", metric, sourceWindow) {
    val stmt = stmtPerWindow(sourceWindow).selects(SliceQuery)
    val boundStmt = stmt.bind(metric.name, Long.box(from.ms), Long.box(to.ms), Int.box(limit))

    val future: Future[ResultSet] = measureFutureTime("bucketSliceCassandra", metric, sourceWindow) {
      session.executeAsync(boundStmt)
    }
    future.map(resultSet ⇒ {
      BucketSlice(resultSet.asScala.flatMap(row ⇒ {
        val ts = row.getLong("timestamp")
        val buckets = getBucketsFromRow(row, sourceWindow)
        buckets.map(serializedBucket ⇒ BucketResult(Timestamp(ts), new LazyBucket(deserialize(sourceWindow, ts, Bytes.getArray(serializedBucket)))))
      }).toSeq)
    })
  }

  def ifNotEmpty(col: Seq[Any])(f: Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future.successful(())
    }
  }

  private def getQueryAsString(stmt: String, binds: Any*): String = {
    s"Query statement: $stmt -> Binds $binds"
  }

  private def getBucketsCollectionType(duration: Duration): String = duration match {
    case Duration(1, millis) ⇒ "list"
    case _                   ⇒ "set"
  }

  private def bucketCollection(bucket: ByteBuffer, duration: Duration): java.util.Collection[ByteBuffer] = duration match {
    case Duration(1, millis) ⇒ Lists.newArrayList(bucket)
    case _                   ⇒ Sets.newHashSet(bucket)
  }

  private def getBucketsFromRow(row: Row, duration: Duration) = duration match {
    case Duration(1, millis) ⇒ row.getList("buckets", classOf[java.nio.ByteBuffer]).asScala
    case _                   ⇒ row.getSet("buckets", classOf[java.nio.ByteBuffer]).asScala
  }

}

object CassandraBucketStore extends ConcurrencySupport {
  implicit val asyncExecutionContext: ExecutionContext = executionContext("bucket-store-worker")
}

