package com.despegar.metrik.store

import com.despegar.metrik.model.Bucket
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import com.despegar.metrik.model.Metric
import com.despegar.metrik.model.CounterBucket

trait CounterBucketStoreSupport extends BucketStoreSupport[CounterBucket] {
  override def bucketStore: BucketStore[CounterBucket] = CassandraBucketStore
}

object CassandraBucketStore extends BucketStore[CounterBucket] {
  override def sliceUntil(metric: Metric, until: Long, windowDuration: Duration): Future[Seq[CounterBucket]] = ???

  override def store(metric: Metric, windowDuration: Duration, buckets: Seq[CounterBucket]): Future[Unit] = ???

  override def remove(metric: Metric, windowDuration: Duration, histogramBuckets: Seq[CounterBucket]): Future[Unit] = ???
}
