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
 * =========================================================================================
 */

package com.searchlight.khronus.store

import java.nio.ByteBuffer

import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ BatchStatement, ResultSet, Session, SimpleStatement }
import com.searchlight.khronus.model.{ Metric, Summary }
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, Measurable, Settings }

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class Slice(from: Long = -1L, to: Long)

object CassandraSummaryStore extends ConcurrencySupport {
  implicit val asyncExecutionContext = executionContext("summary-store-worker")
}

trait SummaryStoreSupport[T <: Summary] {
  def summaryStore: SummaryStore[T]
}

trait SummaryStore[T <: Summary] {
  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit]

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]]

  def readAll(metric: String, windowDuration: Duration, slice: Slice, ascendingOrder: Boolean = true, count: Int): Future[Seq[T]]
}

abstract class CassandraSummaryStore[T <: Summary](session: Session) extends SummaryStore[T] with Logging with Measurable with ConcurrencySupport with CassandraUtils {

  import CassandraSummaryStore.asyncExecutionContext

  protected def tableName(duration: Duration): String

  protected def windowDurations: Seq[Duration] = Settings.Window.WindowDurations

  protected def ttl(windowDuration: Duration): Int

  protected def limit: Int

  protected def fetchSize: Int

  protected def deserialize(timestamp: Long, buffer: Array[Byte]): T

  protected def serializeSummary(summary: T): ByteBuffer

  val QueryAsc = "queryAsc"
  val QueryDesc = "queryDesc"

  windowDurations.foreach(window ⇒ {
    log.info(s"Initializing table ${tableName(window)}")
    retry(MaxRetries, s"Creating ${tableName(window)} table") {
      session.execute(s"create table if not exists ${tableName(window)} (metric text, timestamp bigint, summary blob, primary key (metric, timestamp)) with gc_grace_seconds = 0;")
    }
  })

  val stmtPerWindow: Map[Duration, Statements] = windowDurations.map(windowDuration ⇒ {
    val insert = session.prepare(s"insert into ${tableName(windowDuration)} (metric, timestamp, summary) values (?, ?, ?) using ttl ${ttl(windowDuration)};")

    val simpleStmt = new SimpleStatement(s"select timestamp, summary from ${tableName(windowDuration)} where metric = ? and timestamp >= ? and timestamp <= ? order by timestamp asc limit ?;")
    simpleStmt.setFetchSize(fetchSize)
    val selectAsc = session.prepare(simpleStmt)

    val simpleStmtDesc = new SimpleStatement(s"select timestamp, summary from ${tableName(windowDuration)} where metric = ? and timestamp >= ? and timestamp <= ? order by timestamp desc limit ? ;")
    simpleStmtDesc.setFetchSize(fetchSize)
    val selectDesc = session.prepare(simpleStmtDesc)

    (windowDuration, Statements(insert, Map(QueryAsc -> selectAsc, QueryDesc -> selectDesc), None))
  }).toMap

  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit] = executeChunked(s"summary of $metric-$windowDuration", summaries, Settings.CassandraSummaries.insertChunkSize) {
    summariesChunk ⇒
      log.debug(s"$metric - Storing ${summariesChunk.size} summaries of $windowDuration")
      log.trace(s"$metric - Storing ${summariesChunk.size} summaries ($summariesChunk) of $windowDuration")

      val batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED)
      summariesChunk.foreach(summary ⇒ batchStmt.add(stmtPerWindow(windowDuration).insert.bind(metric.name, Long.box(summary.timestamp.ms), serializeSummary(summary))))

      val future: Future[Unit] = session.executeAsync(batchStmt)
      future
  }

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]] = {
    val slice = Slice(to = now)
    readAll(metric.name, windowDuration, slice)
  }

  def readAll(metric: String, windowDuration: Duration, slice: Slice, ascendingOrder: Boolean = true, count: Int = limit): Future[Seq[T]] = measureFutureTime("summary.readAll", "summary.readAll") {
    log.debug(s"Reading from Cassandra: Cf: $windowDuration - Metric: $metric - From: ${slice.from} - To: ${slice.to} - ascendingOrder: $ascendingOrder - Max results: $count")

    val queryKey = if (ascendingOrder) QueryAsc else QueryDesc
    val boundStmt = stmtPerWindow(windowDuration).selects(queryKey).bind(metric, Long.box(slice.from), Long.box(slice.to), Int.box(count))

    val future: Future[ResultSet] = session.executeAsync(boundStmt)
    future.map(
      resultSet ⇒ resultSet.asScala.map(row ⇒ deserialize(row.getLong("timestamp"), Bytes.getArray(row.getBytes("summary")))).toSeq)
  }

  private def now = System.currentTimeMillis()

}

