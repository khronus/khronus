package com.despegar.metrik.store

import scala.concurrent.duration.Duration
import com.despegar.metrik.model.HistogramBucket

object HistogramBucketStore {

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket] = {
    null
  }

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {

  }

}