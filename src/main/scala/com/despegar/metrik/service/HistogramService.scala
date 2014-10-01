package com.despegar.metrik.service

import org.HdrHistogram.Histogram

/**
 * Created by dberjman on 10/1/14.
 */
class HistogramService {
  // A Histogram covering the range from 1 nsec to 1 hour with 3 decimal point resolution:
  val histogram = new Histogram(3600000000000L, 3);

  def recordTime(duration: Long) = {
    histogram.recordValue(duration)
  }

  def getPercentile(percentile: Double) = {
    histogram.getValueAtPercentile(percentile)
  }

  def getMax() = {
    histogram.getMaxValue
  }
}
