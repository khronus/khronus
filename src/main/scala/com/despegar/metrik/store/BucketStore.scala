package com.despegar.metrik.store

import com.despegar.metrik.model.{ Bucket, HistogramBucket, Metric }

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait BucketStoreSupport {

  def bucketStore: BucketStore
}

trait BucketStore {

  def sliceUntil(metric: Metric, until: Long, windowDuration: Duration): Future[Seq[Bucket]]

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[Bucket]): Future[Unit]

  def remove(metric: Metric, windowDuration: Duration, buckets: Seq[Bucket]): Future[Unit]
}
