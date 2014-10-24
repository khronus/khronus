package com.despegar.metrik.store

import java.util.concurrent.Executors
import com.despegar.metrik.model.{ Summary, StatisticSummary }
import com.despegar.metrik.util.{ Logging, KryoSerializer }
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import com.despegar.metrik.model.Metric

trait StatisticSummarySupport extends SummaryStoreSupport {
  override def summaryStore: SummaryStore = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends SummaryStore with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour)
  //FIXME put configured windows
  val columnFamilies = windowDurations.map(duration ⇒ (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap
  val LIMIT = 1000

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  val serializer: KryoSerializer[StatisticSummary] = new KryoSerializer("statistic", List(StatisticSummary.getClass))

  def initialize = columnFamilies.foreach(cf ⇒ Cassandra.createColumnFamily(cf._2))

  private def getColumnFamilyName(duration: Duration) = s"summary${duration.length}${duration.unit}"

  private def getKey(metric: Metric, windowDuration: Duration): String = s"${metric.name}.${windowDuration.length}${windowDuration.unit}"

  def serializeSummary(summary: StatisticSummary): Array[Byte] = {
    serializer.serialize(summary)
  }

  def store(metric: Metric, windowDuration: Duration, statisticSummaries: Seq[Summary]): Future[Unit] = {
    doUnit(statisticSummaries) {
      Future {
        val mutation = Cassandra.keyspace.prepareMutationBatch()
        val colums = mutation.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
        statisticSummaries.asInstanceOf[Seq[StatisticSummary]].foreach(summary ⇒ colums.putColumn(summary.timestamp, serializeSummary(summary)))

        mutation.execute

        log.debug(s"Store statistics summaries of $windowDuration for metric $metric")
      }
    }
  }

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[StatisticSummary]] = {
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

  def doUnit(col: Seq[Any])(f: ⇒ Future[Unit]): Future[Unit] = {
    if (col.size > 0) {
      f
    } else {
      Future {}
    }
  }

  private def now = System.currentTimeMillis()

  private def infinite = 1L
}