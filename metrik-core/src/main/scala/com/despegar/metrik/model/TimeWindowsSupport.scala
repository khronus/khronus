package com.despegar.metrik.model

import com.despegar.metrik.util.Settings

trait TimeWindowsSupport {
  def histrogramsWindows = Settings.Histogram.TimeWindows
  def countersWindows = Settings.Counter.TimeWindows
}
