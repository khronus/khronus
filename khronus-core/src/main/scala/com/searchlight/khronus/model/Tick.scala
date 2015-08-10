package com.searchlight.khronus.model

import com.searchlight.khronus.util.Settings
import com.searchlight.khronus.util.log.Logging

case class Tick(bucketNumber: BucketNumber) extends Logging {
  def startTimestamp = bucketNumber.startTimestamp()

  def endTimestamp = bucketNumber.endTimestamp()
}

object Tick extends Logging {

  def apply()(implicit clock: Clock = SystemClock): Tick = {
    val executionTimestamp = Timestamp(clock.now)
    val bucketNumber = executionTimestamp.alignedTo(smallestWindow()).fromEndTimestampToBucketNumberOf(smallestWindow())
    val tick = Tick(bucketNumber - Settings.Window.TickDelay)
    tick
  }

  def alreadyProcessed(bucketNumber: BucketNumber)(implicit clock: Clock = SystemClock) = {
    val currentTick = Tick()(new Clock {
      override def now: Long = clock.now + Settings.Master.MaxDelayBetweenClocks.toMillis
    })
    (bucketNumber ~ smallestWindow()) <= currentTick.bucketNumber
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