package com.searchlight.khronus.service

import com.searchlight.khronus.dao.{ Summaries, SummaryStore }
import com.searchlight.khronus.model._
import com.searchlight.khronus.model.query.Query

import scala.concurrent.Future

case class SummaryService() {

  private def getStore(metricType: MetricType): SummaryStore[Summary] = metricType match {
    case Histogram ⇒ getStatisticSummaryStore
    case Counter   ⇒ getCounterSummaryStore
    case Gauge     ⇒ getGaugeSummaryStore
  }

  protected def getStatisticSummaryStore: SummaryStore[Summary] = Summaries.histogramSummaryStore.asInstanceOf[SummaryStore[Summary]]

  protected def getCounterSummaryStore: SummaryStore[Summary] = Summaries.counterSummaryStore.asInstanceOf[SummaryStore[Summary]]

  protected def getGaugeSummaryStore: SummaryStore[Summary] = Summaries.gaugeSummaryStore.asInstanceOf[SummaryStore[Summary]]

  def readSummaries(metric: Metric, query: Query): Future[Seq[Summary]] = {
    getStore(metric.mtype).readAll(metric.flatName, query.resolution.get, query.timeRange.get, query.orderAsc, query.limit)
  }
}
