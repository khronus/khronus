package com.despegar.metrik.store

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.KryoSerializer
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

trait StatisticSummaryStore {
  def store(metric: String, windowDuration: Duration, statisticSummaries: Seq[StatisticSummary]): Future[Unit]
  def sliceUntilNow(metric: String, windowDuration: Duration): Future[Seq[StatisticSummary]]
}

trait StatisticSummarySupport {
  def statisticSummaryStore: StatisticSummaryStore = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends StatisticSummaryStore {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour)
  //FIXME put configured windows
  val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap
  val LIMIT = 1000

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  val serializer: KryoSerializer[StatisticSummary] = new KryoSerializer("statistic", List(StatisticSummary.getClass))

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2))

  private def getColumnFamilyName(duration: Duration) = s"summary${duration.length}${duration.unit}"

  private def getKey(metric: String, windowDuration: Duration): String = s"$metric.${windowDuration.length}${windowDuration.unit}"

  def serializeSummary(summary: StatisticSummary): Array[Byte] = {
    serializer.serialize(summary)
  }

  def store(metric: String, windowDuration: Duration, statisticSummaries: Seq[StatisticSummary]) = {
    Future {
      val mutation = Cassandra.keyspace.prepareMutationBatch()
      val colums = mutation.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
      statisticSummaries.foreach(summary ⇒ colums.putColumn(summary.timestamp, serializeSummary(summary)))

      mutation.execute
    }
  }

  def sliceUntilNow(metric: String, windowDuration: Duration): Future[Seq[StatisticSummary]] = {
    val asyncResult = Future {
      Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(getKey(metric, windowDuration))
        .withColumnRange(infinite, now, false, LIMIT).execute().getResult().asScala
    }

    asyncResult map { slice ⇒
      slice.map { column ⇒
        val timestamp = column.getName()
        val summary: StatisticSummary = serializer.deserialize(column.getByteArrayValue)
        summary
      }.toSeq
    }
  }

  private def now = System.currentTimeMillis()

  private def infinite = 1L
}