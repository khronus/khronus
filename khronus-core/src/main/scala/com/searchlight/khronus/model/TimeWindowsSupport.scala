package com.searchlight.khronus.model

import com.searchlight.khronus.model.MetricSpecs._
import com.searchlight.khronus.util.Settings

trait TimeWindowsSupport {
  val histogramWindows = Settings.Histograms.TimeWindows
  val counterWindows = Settings.Counters.TimeWindows
  val gaugeWindows = Settings.Gauges.TimeWindows
  val windows = Map[(MetricType, MetricSpec), Seq[Window]]((Counter,NonDimensional) -> counterWindows(NonDimensional),
    (Histogram, NonDimensional) -> histogramWindows(NonDimensional),
    (Gauge, NonDimensional) -> gaugeWindows(NonDimensional),
    (Counter,Dimensional) -> counterWindows(Dimensional),
    (Histogram, Dimensional) -> histogramWindows(Dimensional),
    (Gauge, Dimensional) -> gaugeWindows(Dimensional))
  def smallestWindow = Settings.Histograms.TimeWindows(Dimensional).head

}
