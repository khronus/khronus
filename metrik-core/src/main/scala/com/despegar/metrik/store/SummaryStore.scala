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

import com.despegar.metrik.model.{ Metric, Summary }

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import com.despegar.metrik.util.log.Logging
import com.datastax.driver.core.{ SimpleStatement, BatchStatement }
import com.datastax.driver.core.utils.Bytes

case class Slice(from: Long = -1L, to: Long)

trait SummaryStoreSupport[T <: Summary] {
  def summaryStore: SummaryStore[T]
}

trait SummaryStore[T <: Summary] extends Logging {
  private val Limit = 1000
  private val FetchSize = 200

  def windowDurations: Seq[Duration]

  def tableName(duration: Duration): String

  def deserialize(timestamp: Long, buffer: Array[Byte]): T

  def serializeSummary(summary: T): ByteBuffer

  protected def ttl(windowDuration: Duration): Int

  lazy val tables: Map[Duration, String] = windowDurations.map(duration ⇒ (duration, tableName(duration))).toMap

  def initialize = tables.values.foreach(tableName ⇒ {
    log.info(s"Initializing table $tableName")
    Cassandra.session.execute(s"create table if not exists $tableName (metric text, timestamp bigint, summary blob, primary key (metric, timestamp));")
  })

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  import Cassandra._

  private def now = System.currentTimeMillis()

  def ifNotEmpty(col: Seq[Any])(f: ⇒ Unit): Future[Unit] = {
    if (col.size > 0) {
      Future {
        f
      }
    } else {
      Future.successful(())
    }
  }

  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit] = {
    ifNotEmpty(summaries) {
      log.debug(s"$metric - Storing ${summaries.size} summaries ($summaries) of $windowDuration")

      val insertStmt = Cassandra.session.prepare(s"insert into ${tables(windowDuration)} (metric, timestamp, summary) values (?, ?, ?) using ttl ${ttl(windowDuration)}; ")
      val batchStmt = new BatchStatement();

      summaries.foreach(summary ⇒ batchStmt.add(insertStmt.bind(metric.name, Long.box(summary.timestamp.ms), serializeSummary(summary))))

      Cassandra.session.executeAsync(batchStmt)
    }
  }

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]] = {
    val slice = Slice(to = now)
    readAll(metric.name, windowDuration, slice)
  }

  def readAll(metric: String, windowDuration: Duration, slice: Slice, ascendingOrder: Boolean = true, count: Int = Limit): Future[Seq[T]] = {
    log.info(s"Reading from Cassandra: Cf: $windowDuration - Metric: $metric - From: ${slice.from} - To: ${slice.to} - ascendingOrder: $ascendingOrder - Max results: $count")

    val order = if (ascendingOrder) "asc" else "desc"
    val query = s"select timestamp, summary from ${tables(windowDuration)} where metric = ? and timestamp >= ? and timestamp <= ? order by timestamp $order limit ?;"

    val stmt = new SimpleStatement(query)
    stmt.setFetchSize(FetchSize)
    val preparedStmt = Cassandra.session.prepare(stmt).bind(metric, Long.box(slice.from), Long.box(slice.to), Int.box(count))

    Cassandra.session.executeAsync(preparedStmt).map(
      resultSet ⇒ resultSet.asScala.map(row ⇒ deserialize(row.getLong("timestamp"), Bytes.getArray(row.getBytes("summary")))).toSeq)
  }

}
