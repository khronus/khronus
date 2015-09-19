package com.searchlight.khronus.model

import com.searchlight.khronus.util.Settings

trait TimeWindowsSupport {
  val histogramsWindows = Settings.Histogram.TimeWindows
  val countersWindows = Settings.Counter.TimeWindows
  val windows = Map[String, Seq[Window]](MetricType.Counter -> countersWindows, MetricType.Timer -> histogramsWindows, MetricType.Gauge -> histogramsWindows)
  def smallestWindow = Settings.Histogram.TimeWindows(0)
}
