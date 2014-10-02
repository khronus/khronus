package com.despegar.metrik.model

import com.despegar.metrik.store.HistogramBucketStore

trait HistogramBucketSupport {

  def histogramBucketStore: HistogramBucketStore = HistogramBucketStore

}
