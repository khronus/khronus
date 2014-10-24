package com.despegar.metrik.model

import scala.concurrent.duration.Duration

abstract case class Bucket(bucketNumber: Long, duration: Duration) {
  def timestamp = bucketNumber * duration.toMillis

  def summary: Summary
}
