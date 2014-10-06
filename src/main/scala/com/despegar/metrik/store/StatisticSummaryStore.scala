package com.despegar.metrik.store

import com.despegar.metrik.model.StatisticSummary

trait StatisticSummaryStore {
  def store(statisticSummaries: Seq[StatisticSummary])
}
object CassandraStatisticSummaryStore extends StatisticSummaryStore {

  def store(statisticSummaries: Seq[StatisticSummary]) = {
    
  }
}