package com.despegar.metrik.model

import com.despegar.metrik.util.Settings

trait TimeWindowsSupport {
  val histrogramsWindows = Settings().Histogram.timeWindows
  val countersWindows = Settings().Counter.timeWindows
}
