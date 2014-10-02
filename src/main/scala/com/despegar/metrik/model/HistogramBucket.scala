package com.despegar.metrik.model

import org.HdrHistogram.Histogram

import scala.concurrent.duration.Duration

case class HistogramBucket(bucketNumer: Long, duration: Duration, histogram: Histogram) {
  def timestamp = bucketNumer * duration.toMillis
}