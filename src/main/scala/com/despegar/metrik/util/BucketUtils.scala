package com.despegar.metrik.util

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

object BucketUtils {

  /**
   * Get the actual bucket timestamp (upper limit).
   */
  def getCurrentBucketTimestamp(duration: Duration, executionTimestamp: Long) = {
    (executionTimestamp / duration.toMillis) * duration.toMillis
  }

  private def now = System.currentTimeMillis()
}
