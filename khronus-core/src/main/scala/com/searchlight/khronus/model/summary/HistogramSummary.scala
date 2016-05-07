package com.searchlight.khronus.model.summary

import com.searchlight.khronus.model.{ Summary, Timestamp }

case class HistogramSummary(timestamp: Timestamp, p50: Long, p80: Long, p90: Long, p95: Long, p99: Long, p999: Long, min: Long, max: Long, count: Long, mean: Long) extends Summary {
  override def toString = s"HistogramSummary(timestamp=${timestamp.ms},count=$count,...)"
}
