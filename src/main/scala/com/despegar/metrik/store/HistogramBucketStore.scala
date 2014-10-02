package com.despegar.metrik.store

import scala.concurrent.duration.Duration
import com.despegar.metrik.model.HistogramBucket

trait HistogramBucketStore {

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket]
  
  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])
  
}

object HistogramBucketStore extends  HistogramBucketStore {
  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket] = {
    //cassandra: row[metric]
    null
  }

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {

  }
}