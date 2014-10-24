package com.despegar.metrik.store

import com.despegar.metrik.model.{ Summary, StatisticSummary, Metric }

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait SummaryStoreSupport {

  def summaryStore: SummaryStore
}

trait SummaryStore {

  def store(metric: Metric, windowDuration: Duration, summaries: Seq[Summary]): Future[Unit]

  def sliceUntilNow(metric: Metric, windowDuration: Duration): Future[Seq[Summary]]
}
