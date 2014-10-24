package com.despegar.metrik.store

import com.despegar.metrik.model.Bucket

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait CounterBucketStore extends BucketStore {
}

trait CounterBucketStoreSupport extends BucketStoreSupport {
  override def bucketStore = CassandraBucketStore
}

object CassandraBucketStore extends CounterBucketStore {
  override def sliceUntil(metric: String, until: Long, sourceWindow: Duration): Future[Seq[Bucket]] = ???

  override def store(metric: String, windowDuration: Duration, buckets: Seq[Bucket]): Future[Unit] = ???

  override def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[Bucket]): Future[Unit] = ???
}
