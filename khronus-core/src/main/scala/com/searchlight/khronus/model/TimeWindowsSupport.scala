package com.searchlight.khronus.model

import com.searchlight.khronus.util.Settings

trait TimeWindowsSupport {
  val histogramWindows = Settings.Histograms.TimeWindows
  val counterWindows = Settings.Counters.TimeWindows
  val gaugeWindows = Settings.Gauges.TimeWindows
  val windows = Map[MetricType, Seq[Window]](Counter -> counterWindows, Histogram -> histogramWindows,
    Gauge -> gaugeWindows)
  def smallestWindow = Settings.Histograms.TimeWindows.head
}
