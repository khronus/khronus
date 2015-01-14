package com.despegar.khronus.model

import com.despegar.khronus.util.Settings
import com.despegar.khronus.util.log.Logging

case class Tick(bucketNumber: BucketNumber) extends Logging {
  def startTimestamp = bucketNumber.startTimestamp()
  def endTimestamp = bucketNumber.endTimestamp()

  override def toString = s"Tick($bucketNumber)"
}

object Tick extends Logging {
  def current(windows: Seq[TimeWindow[_, _]]): Tick = {

    //the effectiveNow corresponds to the end of the current Tick. Hence, we need a BucketNumber ending at effectiveNow.
    val tickBucketNumber = effectiveNow(windows).fromEndTimestampToBucketNumberOf(firstDurationOf(windows))
    val tick = Tick(tickBucketNumber)
    log.debug(s"$tick")
    tick
  }
  def now = System.currentTimeMillis()

  /**
   * The effectiveNow Timestamp is the current wall clock time after delaying it with ExecutionDelay and aligning to the first TimeWindow.
   * It corresponds to the end timestamp of the current Tick.
   */
  def effectiveNow(windows: Seq[TimeWindow[_, _]]): Timestamp = {
    val delayedExecutionTimestamp = Timestamp(now - Settings.Window.ExecutionDelay)
    log.debug(s"Building Tick for delayedExecutionTimestamp ${date(delayedExecutionTimestamp.ms)}")

    //the aligned delayedExecutionTimestamp is essentially the effectiveNow() and corresponds to the end of the current Tick
    delayedExecutionTimestamp.alignedTo(firstDurationOf(windows))
  }

  private def firstDurationOf(windows: Seq[TimeWindow[_, _]]) = windows(0).duration
}