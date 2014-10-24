package com.despegar.metrik.store

import com.despegar.metrik.model.{ Bucket, HistogramBucket }

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait BucketStoreSupport {

  def bucketStore: BucketStore
}

trait BucketStore {

  def sliceUntil(metric: String, until: Long, sourceWindow: Duration): Future[Seq[Bucket]]

  def store(metric: String, windowDuration: Duration, buckets: Seq[Bucket]): Future[Unit]

  def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[Bucket]): Future[Unit]
}
