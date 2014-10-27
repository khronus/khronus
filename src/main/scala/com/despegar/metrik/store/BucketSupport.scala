package com.despegar.metrik.store

import com.despegar.metrik.model.Bucket

trait BucketSupport {

  val histogramBucketStore = CassandraHistogramBucketStore
  val counterBucketStore = CassandraCounterBucketStore

}