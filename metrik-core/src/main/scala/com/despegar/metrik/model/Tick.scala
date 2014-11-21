package com.despegar.metrik.model

import com.despegar.metrik.util.Settings
import com.despegar.metrik.util.log.Logging

case class Tick(bucketNumber: BucketNumber) extends Logging {
  def startTimestamp = bucketNumber.startTimestamp()
  def endTimestamp = bucketNumber.endTimestamp()

  override def toString = s"Tick($bucketNumber)"
}

object Tick extends Logging {
  def current(windows: Seq[TimeWindow[_, _]]): Tick = {
    val executionTimestamp = Timestamp(now - Settings().Window.ExecutionDelay)
    log.debug(s"Building Tick for executionTimestamp ${date(executionTimestamp.ms)}")
    val bucketNumber = executionTimestamp.alignedTo(firstDurationOf(windows)).toBucketNumber(firstDurationOf(windows))
    val tick = Tick(bucketNumber - 1)
    log.debug(s"$tick")
    tick
  }
  def now = System.currentTimeMillis()

  private def firstDurationOf(windows: Seq[TimeWindow[_, _]]) = windows(0).duration
}