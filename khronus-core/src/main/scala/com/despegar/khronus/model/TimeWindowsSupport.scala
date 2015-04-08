package com.despegar.khronus.model

import com.despegar.khronus.util.Settings

trait TimeWindowsSupport {
  val histrogramsWindows = Settings.Histogram.TimeWindows
  val countersWindows = Settings.Counter.TimeWindows
  val windows = Map(MetricType.Counter -> countersWindows, MetricType.Timer -> histrogramsWindows, MetricType.Gauge -> histrogramsWindows)
}
