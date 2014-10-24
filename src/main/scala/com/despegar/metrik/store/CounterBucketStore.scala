package com.despegar.metrik.store

import com.despegar.metrik.model.Bucket
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.Metric

trait CounterBucketStore extends BucketStore

trait CounterBucketStoreSupport extends BucketStoreSupport {
  override def bucketStore = CassandraBucketStore
}

object CassandraBucketStore extends CounterBucketStore {
  override def sliceUntil(metric: Metric, until: Long, windowDuration: Duration): Future[Seq[Bucket]] = ???

  override def store(metric: Metric, windowDuration: Duration, buckets: Seq[Bucket]): Future[Unit] = ???

  override def remove(metric: Metric, windowDuration: Duration, histogramBuckets: Seq[Bucket]): Future[Unit] = ???
}
