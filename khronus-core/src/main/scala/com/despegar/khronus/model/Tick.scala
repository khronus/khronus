package com.despegar.khronus.model

import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging

case class Tick(bucketNumber: BucketNumber) extends Logging {
  def startTimestamp = bucketNumber.startTimestamp()

  def endTimestamp = bucketNumber.endTimestamp()
}

object Tick extends Logging {

  def current(implicit clock: Clock = SystemClock): Tick = {
    val executionTimestamp = Timestamp(clock.now)
    val bucketNumber = executionTimestamp.alignedTo(smallestWindow()).fromEndTimestampToBucketNumberOf(smallestWindow())
    val tick = Tick(bucketNumber - Settings.Window.TickDelay)
    log.debug(s"$tick")
    tick
  }

  def smallestWindow() = Settings.Histogram.TimeWindows.head.duration

  def highestWindow() = Settings.Histogram.TimeWindows.last.duration
}

trait Clock {
  def now: Long
}

object SystemClock extends Clock {
  def now: Long = System.currentTimeMillis()
}