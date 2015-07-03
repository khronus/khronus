package com.despegar.khronus.util

import com.despegar.khronus.model._
import com.despegar.khronus.store.{ BucketCache, BucketCacheSupport }

trait BucketCacheSupportMock extends BucketCacheSupport {
  override val bucketCache: BucketCache = new BucketCacheMock()
}

class BucketCacheMock extends BucketCache {
  override def markProcessedTick(metric: Metric, tick: Tick): Unit = {}

  override def multiGet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = None

  override def multiSet(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit = {}
}

