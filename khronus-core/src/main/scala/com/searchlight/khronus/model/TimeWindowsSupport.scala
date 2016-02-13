package com.searchlight.khronus.model

import com.searchlight.khronus.util.Settings

trait TimeWindowsSupport {
  val histogramsWindows = Settings.Histograms.TimeWindows
  val countersWindows = Settings.Counters.TimeWindows
  val windows = Map[MetricType, Seq[Window]](Counter -> countersWindows, Histogram -> histogramsWindows)
  def smallestWindow = Settings.Histograms.TimeWindows.head
}
