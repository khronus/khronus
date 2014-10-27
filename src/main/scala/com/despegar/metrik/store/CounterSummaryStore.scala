package com.despegar.metrik.store

import com.despegar.metrik.model.Summary
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.Metric

trait CounterSummaryStoreSupport extends SummaryStoreSupport {
  override def summaryStore: SummaryStore = CassandraCounterSummaryStore
}

trait CounterSummaryStore extends SummaryStore

object CassandraCounterSummaryStore extends CounterSummaryStore {
  override def store(metric: Metric, windowDuration: Duration, summaries: Seq[Summary]): Future[Unit] = ???

  override def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[Summary]] = ???
}
