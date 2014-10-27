package com.despegar.metrik.store

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.{ KryoSerializer, Logging }

import scala.concurrent.duration._

trait StatisticSummarySupport extends SummaryStoreSupport[StatisticSummary] {
  override def summaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends SummaryStore[StatisticSummary] with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour)

  val serializer: KryoSerializer[StatisticSummary] = new KryoSerializer("statisticSummary", List(StatisticSummary.getClass))

  override def getColumnFamilyName(duration: Duration) = s"statisticSummary${duration.length}${duration.unit}"

  override def serializeSummary(summary: StatisticSummary): Array[Byte] = {
    serializer.serialize(summary)
  }

  override def deserialize(bytes: Array[Byte]): StatisticSummary = {
    serializer.deserialize(bytes)
  }
}