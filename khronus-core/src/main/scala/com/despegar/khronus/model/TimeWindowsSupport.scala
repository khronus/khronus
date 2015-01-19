package com.despegar.khronus.model

import com.despegar.khronus.util.Settings

trait TimeWindowsSupport {
  def histrogramsWindows = Settings.Histogram.TimeWindows
  def countersWindows = Settings.Counter.TimeWindows

  def smallestWindow = Settings.Histogram.TimeWindows(0)
}
