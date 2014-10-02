package com.despegar.metrik.model

import com.despegar.metrik.store.HistogramBucketStore
import com.despegar.metrik.store.CassandraHistogramBucketStore

trait HistogramBucketSupport {

  def histogramBucketStore: HistogramBucketStore = CassandraHistogramBucketStore

}
