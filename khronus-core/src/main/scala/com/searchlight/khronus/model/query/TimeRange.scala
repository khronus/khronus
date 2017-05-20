package com.searchlight.khronus.model.query

import com.searchlight.khronus.util.Settings

import scala.concurrent.duration.Duration

import scala.concurrent.duration._

case class TimeRange(from: Long = 0L, to: Long) {

  def mergedWith(other: TimeRange): TimeRange = {
    TimeRange(if (from > other.from) from else other.from, if (to < other.to) to else other.to)
  }

  def duration: Duration = (to - from) millis

  def getAdjustedResolution(resolution: Duration, forceResolution: Boolean, minResolution: Int, maxResolution: Int): Duration = {
    val sortedWindows = Settings.Window.ConfiguredWindows.toSeq.sortBy(_.toMillis).reverse
    val nearestConfiguredWindow = sortedWindows.foldLeft(sortedWindows.last)((nearest, next) ⇒ if (millisBetween(resolution, next) < millisBetween(resolution, nearest)) next else nearest)

    if (forceResolution) {
      nearestConfiguredWindow
    } else {
      val points = resolutionPoints(nearestConfiguredWindow)
      if (points <= maxResolution & points >= minResolution)
        nearestConfiguredWindow
      else {
        sortedWindows.foldLeft(sortedWindows.head)((adjustedWindow, next) ⇒ {
          val points = resolutionPoints(next)
          if (points >= minResolution & points <= maxResolution)
            next
          else if (points < minResolution) next else adjustedWindow
        })
      }
    }
  }

  private def millisBetween(some: Duration, other: Duration) = Math.abs(some.toMillis - other.toMillis)

  private def resolutionPoints(timeWindow: Duration) = {
    Math.abs(to - from) / timeWindow.toMillis
  }
}
