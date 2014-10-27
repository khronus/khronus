package com.despegar.metrik.store

import com.despegar.metrik.model.CounterSummary
import com.despegar.metrik.util.KryoSerializer

import scala.concurrent.duration._

trait CounterSummaryStoreSupport extends SummaryStoreSupport[CounterSummary] {
  override def summaryStore = CounterSummaryStore
}

object CounterSummaryStore extends SummaryStore[CounterSummary] {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(30 seconds)

  val serializer: KryoSerializer[CounterSummary] = new KryoSerializer("counterSummary", List(CounterSummary.getClass))

  override def getColumnFamilyName(duration: Duration) = s"counterSummary${duration.length}${duration.unit}"

  override def serializeSummary(summary: CounterSummary): Array[Byte] = {
    serializer.serialize(summary)
  }

  override def deserialize(bytes: Array[Byte]): CounterSummary = {
    serializer.deserialize(bytes)
  }
}
