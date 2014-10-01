package com.despegar.metrik.model

import org.HdrHistogram.Histogram

case class HistogramBucket(timestamp: Long, histogram: Histogram)