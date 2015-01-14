package com.despegar.khronus.util

import com.despegar.khronus.model._
import com.despegar.khronus.store.{ BucketCache, BucketCacheSupport }

trait BucketCacheSupportMock extends BucketCacheSupport {
  override val bucketCache: BucketCache = new BucketCacheMock()
}

class BucketCacheMock extends BucketCache {
  override def markProcessedTick(metric: Metric, tick: Tick): Unit = {}

  override def take[T](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[Seq[(Timestamp, () ⇒ T)]] = None

  override def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit = {}
}

