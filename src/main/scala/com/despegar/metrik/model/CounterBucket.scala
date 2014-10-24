package com.despegar.metrik.model

import scala.concurrent.duration.Duration

class CounterBucket(override val bucketNumber: Long, override val duration: Duration, val counts: Long) extends Bucket(bucketNumber, duration) {

  override def summary: CounterSummary = {
    null
  }
}
