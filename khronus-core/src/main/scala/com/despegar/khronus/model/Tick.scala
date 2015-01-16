package com.despegar.khronus.model

import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging

import scala.concurrent.duration.Duration

case class Tick(bucketNumber: BucketNumber) extends Logging {

  private val windowsToBeExecuted = Settings.Window.ConfiguredWindows.filter(windowDuration ⇒ mustExecute(windowDuration))

  def mustExecute(timeWindow: TimeWindow[_, _]): Boolean = {
    windowsToBeExecuted.contains(timeWindow.duration)
  }

  private def mustExecute(windowDuration: Duration): Boolean = (endTimestamp.ms % windowDuration.toMillis) == 0

  def startTimestamp = bucketNumber.startTimestamp()

  def endTimestamp = bucketNumber.endTimestamp()

  override def toString = s"Tick($bucketNumber)"
}

object Tick extends Logging {
  def now = System.currentTimeMillis()

  def apply(): Tick = {
    //the effectiveNow corresponds to the end of the current Tick. Hence, we need a BucketNumber ending at effectiveNow.
    val tickBucketNumber = effectiveNow().fromEndTimestampToBucketNumberOf(firstWindowDuration())
    val tick = Tick(tickBucketNumber)
    log.debug(s"$tick")
    tick
  }

  /**
   * The effectiveNow Timestamp is the current wall clock time after delaying it with ExecutionDelay and aligning to the first TimeWindow.
   * It corresponds to the end timestamp of the current Tick.
   */
  def effectiveNow(): Timestamp = {
    val delayedExecutionTimestamp = Timestamp(now - Settings.Window.ExecutionDelay)
    log.debug(s"Building Tick for delayedExecutionTimestamp ${date(delayedExecutionTimestamp.ms)}")

    //the aligned delayedExecutionTimestamp is essentially the effectiveNow() and corresponds to the end of the current Tick
    delayedExecutionTimestamp.alignedTo(firstWindowDuration())
  }

  private def firstWindowDuration() = Settings.Window.ConfiguredWindows(0)
}