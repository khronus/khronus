package com.despegar.khronus.model

import com.despegar.khronus.util.Settings

trait TimeWindowsSupport {
  val histogramsWindows = Settings.Histogram.TimeWindows
  val countersWindows = Settings.Counter.TimeWindows
  val windows = Map(MetricType.Counter -> countersWindows, MetricType.Timer -> histogramsWindows, MetricType.Gauge -> histogramsWindows)
  def smallestWindow = Settings.Histogram.TimeWindows(0)
}
