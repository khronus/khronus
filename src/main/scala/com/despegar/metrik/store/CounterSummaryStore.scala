package com.despegar.metrik.store

import com.despegar.metrik.model.Summary

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait CounterSummaryStoreSupport extends SummaryStoreSupport {
  override def summaryStore = CounterSummaryStore
}

object CounterSummaryStore extends SummaryStore {
  override def store(metric: String, windowDuration: Duration, summaries: Seq[Summary]): Future[Unit] = ???

  override def sliceUntilNow(metric: String, windowDuration: Duration): Future[Seq[Summary]] = ???
}
