package com.despegar.metrik.model

import com.despegar.metrik.util.Settings

trait TimeWindowsSupport {
  def histrogramsWindows = Settings().Histogram.timeWindows
  def countersWindows = Settings().Counter.timeWindows
}
