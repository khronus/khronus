package com.despegar.metrik.model

import com.despegar.metrik.util.log.Logging

import scala.concurrent.duration.Duration

object ExtraLog extends Logging {
  def logthis(metricName: String, summaries: Seq[Summary with Product with Serializable]) = {
    if (metricName.equalsIgnoreCase("metricsReceived")) {
      log.info(s"Extra $summaries")
    }
  }

  def logthis(metric: Metric, summaries: Seq[Summary], windowDuration: Duration) = {
    if (metric.name.equalsIgnoreCase("metricsReceived")) {
      log.info(s"Extra $metric - Storing ${summaries.size} summaries ($summaries) of $windowDuration")
    }
  }

}
