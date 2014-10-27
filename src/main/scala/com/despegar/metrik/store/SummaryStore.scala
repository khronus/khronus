package com.despegar.metrik.store

import java.util.concurrent.Executors

import com.despegar.metrik.model.{ Metric, Summary }
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }

trait SummaryStoreSupport[T <: Summary] {

  def summaryStore: SummaryStore[T]
}

trait SummaryStore[T <: Summary] extends Logging {
  private val LIMIT = 1000
  private val INFINITE = 1L

  def windowDurations: Seq[Duration]

  def getColumnFamilyName(duration: Duration): String

  lazy val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2))

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  def getKey(metric: Metric, windowDuration: Duration): String = metric.name

  def doUnit(col: Seq[Any])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future {}
    }
  }

  def serializeSummary(summary: T): Array[Byte]

  def store(metric: Metric, windowDuration: Duration, summaries: Seq[T]): Future[Unit] = {
    doUnit(summaries) {
      Future {
        val mutation = Cassandra.keyspace.prepareMutationBatch()
        val colums = mutation.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
        summaries.foreach(summary ⇒ colums.putColumn(summary.getTimestamp, serializeSummary(summary)))

        mutation.execute

        log.debug(s"Store summaries of $windowDuration for metric $metric")
      }
    }
  }

  private def now = System.currentTimeMillis()

  def deserialize(bytes: Array[Byte]): T

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[T]] = {
    val asyncResult = Future {
      Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(getKey(metric, windowDuration))
        .withColumnRange(INFINITE, now, false, LIMIT).execute().getResult().asScala
    }

    asyncResult map { slice ⇒
      slice.map { column ⇒
        val timestamp = column.getName()
        val summary: T = deserialize(column.getByteArrayValue)
        summary
      }.toSeq
    }
  }
}
