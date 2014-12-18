/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.store

import java.nio.ByteBuffer

import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ BatchStatement, Session, SimpleStatement }
import com.despegar.khronus.model.{ Metric, Summary }
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ ConcurrencySupport, Measurable }

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure

case class Slice(from: Long = -1L, to: Long)

trait SummaryStoreSupport[T <: Summary] {
  def summaryStore: SummaryStore[T]
}

trait SummaryStore[T <: Summary] {
  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit]

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]]

  def readAll(metric: String, windowDuration: Duration, slice: Slice, ascendingOrder: Boolean = true, count: Int): Future[Seq[T]]
}

abstract class CassandraSummaryStore[T <: Summary](session: Session) extends SummaryStore[T] with Logging with Measurable with ConcurrencySupport with CassandraUtils {

  protected def tableName(duration: Duration): String

  protected def windowDurations: Seq[Duration]

  protected def ttl(windowDuration: Duration): Int

  protected def limit: Int

  protected def fetchSize: Int

  protected def deserialize(timestamp: Long, buffer: Array[Byte]): T

  protected def serializeSummary(summary: T): ByteBuffer

  implicit val asyncExecutionContext = executionContext("summary-store-worker", 50)

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

  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit] = {
    ifNotEmpty(summaries) {
      measureFutureTime("storeSummaries", metric, windowDuration) {
        log.debug(s"$metric - Storing ${summaries.size} summaries ($summaries) of $windowDuration")

        val batchStmt = new BatchStatement();
        summaries.foreach(summary ⇒ batchStmt.add(stmtPerWindow(windowDuration).insert.bind(metric.name, Long.box(summary.timestamp.ms), serializeSummary(summary))))

        toFutureUnit {
          session.executeAsync(batchStmt).
            andThen { case Failure(reason) ⇒ log.error(s"$metric - Storing summary of $windowDuration failed", reason) }
        }
      }
    }
  }

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]] = {
    val slice = Slice(to = now)
    readAll(metric.name, windowDuration, slice)
  }

  def readAll(metric: String, windowDuration: Duration, slice: Slice, ascendingOrder: Boolean = true, count: Int = limit): Future[Seq[T]] = {
    log.info(s"Reading from Cassandra: Cf: $windowDuration - Metric: $metric - From: ${slice.from} - To: ${slice.to} - ascendingOrder: $ascendingOrder - Max results: $count")

    val queryKey = if (ascendingOrder) QueryAsc else QueryDesc
    val boundStmt = stmtPerWindow(windowDuration).selects(queryKey).bind(metric, Long.box(slice.from), Long.box(slice.to), Int.box(count))

    session.executeAsync(boundStmt).map(
      resultSet ⇒ resultSet.asScala.map(row ⇒ deserialize(row.getLong("timestamp"), Bytes.getArray(row.getBytes("summary")))).toSeq)
  }

  private def now = System.currentTimeMillis()

  def ifNotEmpty(col: Seq[Any])(f: Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future.successful(())
    }
  }

}
