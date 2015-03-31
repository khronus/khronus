package com.despegar.khronus.model

import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging

case class Tick(bucketNumber: BucketNumber) extends Logging {
  def startTimestamp = bucketNumber.startTimestamp()
  def endTimestamp = bucketNumber.endTimestamp()

  override def toString = s"Tick($bucketNumber)"
}

object Tick extends Logging {

  def current(): Tick = {
    val executionTimestamp = Timestamp(now - Settings.Window.ExecutionDelay)
    log.debug(s"Building Tick for executionTimestamp ${date(executionTimestamp.ms)}")
    val bucketNumber = executionTimestamp.alignedTo(smallestWindow()).toBucketNumber(smallestWindow())
    val tick = Tick(bucketNumber - 1)
    log.debug(s"$tick")
    tick
  }

  def now = System.currentTimeMillis()

  private def smallestWindow() = Settings.Histogram.TimeWindows.head.duration
}