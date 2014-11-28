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
import com.despegar.metrik.model.{ Bucket, Metric, Timestamp }
import com.despegar.metrik.util.Measurable
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Success, Failure }
import com.despegar.metrik.util.log.Logging
import com.datastax.driver.core.{ PreparedStatement, SimpleStatement, BatchStatement }
import com.datastax.driver.core.utils.Bytes
import java.util

trait BucketStoreSupport[T <: Bucket] {

  def bucketStore: BucketStore[T]
}

trait BucketStore[T <: Bucket] extends Logging with Measurable {

  import Cassandra._

  protected def tableName(duration: Duration): String
  protected def windowDurations: Seq[Duration]
  protected def ttl(windowDuration: Duration): Int
  protected def limit: Int
  protected def fetchSize: Int

  protected def toBucket(windowDuration: Duration, timestamp: Long, counts: Array[Byte]): T
  protected def serializeBucket(metric: Metric, windowDuration: Duration, bucket: T): ByteBuffer

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  val SliceQuery = "sliceQuery"

  lazy val stmtPerWindow: Map[Duration, Statements] = windowDurations.map(windowDuration ⇒ {
    val insert = Cassandra.session.prepare(s"update ${tableName(windowDuration)} using ttl ${ttl(windowDuration)} set buckets = buckets + ? where metric = ? and timestamp = ? ; ")

    val simpleStmt = new SimpleStatement(s"select timestamp, buckets from ${tableName(windowDuration)} where metric = ? and timestamp >= ? and timestamp <= ? limit ?;")
    simpleStmt.setFetchSize(fetchSize)
    val select = Cassandra.session.prepare(simpleStmt)

    val delete = Cassandra.session.prepare(s"delete from ${tableName(windowDuration)} where metric = ? and timestamp = ?;")
    (windowDuration, Statements(insert, Map(SliceQuery -> select), Some(delete)))
  }).toMap

  def initialize = windowDurations.foreach(window ⇒ {
    log.info(s"Initializing table ${tableName(window)}")
    Cassandra.session.execute(s"create table if not exists ${tableName(window)} (metric text, timestamp bigint, buckets list<blob>, primary key (metric, timestamp));")
  })

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit] = {
    ifNotEmpty(buckets) {
      log.debug(s"${p(metric, windowDuration)} - Storing ${buckets.length} buckets")

      val batchStmt = new BatchStatement();
      buckets.foreach(bucket ⇒ {
        val serializedBucket = serializeBucket(metric, windowDuration, bucket)
        log.info(s"${p(metric, windowDuration)} Storing a bucket of ${serializedBucket.limit()} bytes")
        batchStmt.add(stmtPerWindow(windowDuration).insert.bind(Seq(serializedBucket).asJava, metric.name, Long.box(bucket.timestamp.ms)))
      })

      Cassandra.session.executeAsync(batchStmt).andThen {
        case Failure(reason) ⇒ log.error(s"$metric - Storing metrics ${metric.name} failed", reason)
      }
    }
  }

  def remove(metric: Metric, windowDuration: Duration, bucketTimestamps: Seq[Timestamp]): Future[Unit] = {
    ifNotEmpty(bucketTimestamps) {
      log.debug(s"${p(metric, windowDuration)} - Removing ${bucketTimestamps.length} buckets")

      val batchStmt = new BatchStatement();
      bucketTimestamps.foreach(bucket ⇒ batchStmt.add(stmtPerWindow(windowDuration).delete.get.bind(metric.name, Long.box(bucket.ms))))

      Cassandra.session.executeAsync(batchStmt).andThen {
        case Failure(reason) ⇒ log.error(s"$metric - Removing metrics ${metric.name} failed", reason)
      }
    }
  }

  def slice(metric: Metric, from: Timestamp, to: Timestamp, sourceWindow: Duration): Future[Seq[(Timestamp, () ⇒ T)]] = {
    val boundStmt = stmtPerWindow(sourceWindow).selects(SliceQuery).bind(metric.name, Long.box(from.ms), Long.box(to.ms), Int.box(limit))

    Cassandra.session.executeAsync(boundStmt).map(resultSet ⇒ {
      resultSet.asScala.flatMap(row ⇒ {
        val ts = row.getLong("timestamp")
        val buckets = row.getList("buckets", classOf[java.nio.ByteBuffer])
        buckets.asScala.map(serializedBucket ⇒ (Timestamp(ts), () ⇒ toBucket(sourceWindow, ts, Bytes.getArray(serializedBucket))))
      }).toSeq
    })
  }

  def ifNotEmpty(col: Seq[Any])(f: ⇒ Unit): Future[Unit] = {
    if (col.size > 0) {
      Future {
        f
      }
    } else {
      Future.successful(())
    }
  }

}
