package com.despegar.metrik.store

import com.despegar.metrik.model.Summary
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.Metric

trait CounterSummaryStoreSupport extends SummaryStoreSupport {
  override def summaryStore = CounterSummaryStore
}

object CounterSummaryStore extends SummaryStore {
  override def store(metric: Metric, windowDuration: Duration, summaries: Seq[Summary]): Future[Unit] = ???

  override def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[Summary]] = ???
}
