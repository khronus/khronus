package com.despegar.metrik.store

import com.despegar.metrik.model.{ Bucket, HistogramBucket, Metric }

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait BucketStoreSupport[T <: Bucket] {

  def bucketStore: BucketStore[T]
}

trait BucketStore[T <: Bucket] {

  def sliceUntil(metric: Metric, until: Long, windowDuration: Duration): Future[Seq[T]]

  def store(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit]

  def remove(metric: Metric, windowDuration: Duration, buckets: Seq[T]): Future[Unit]
}
