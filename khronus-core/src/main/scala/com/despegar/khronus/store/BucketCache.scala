package com.despegar.khronus.store

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.despegar.khronus.model._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{Measurable, Settings}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait BucketCacheSupport {
  val bucketCache: BucketCache = InMemoryBucketCache
}

trait BucketCache {

  def markProcessedTick(metric: Metric, tick: Tick): Unit

  def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit

  def take[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]]

}

object InMemoryBucketCache extends BucketCache with Logging with Measurable {

  type MetricCache = ConcurrentHashMap[BucketNumber, Bucket]
  private val cachesByMetric = new ConcurrentHashMap[Metric, MetricCache]()
  private val lastKnownTick = new AtomicReference[Tick]()

  private val enabled = Settings.BucketCache.Enabled

  def markProcessedTick(metric: Metric, tick: Tick) = if (enabled) {
    val previousKnownTick = lastKnownTick.getAndSet(tick)
    if (previousKnownTick != tick && previousKnownTick != null) {
      cachesByMetric.keySet().asScala.foreach { metric ⇒
        if (noCachedBucketFor(metric, previousKnownTick.bucketNumber)) {
          incrementCounter("bucketCache.noMetricAffinity")
          cleanCache(metric)
        }
      }
    }
  }

  private def noCachedBucketFor(metric: Metric, bucketNumber: BucketNumber): Boolean = {
    !metricCacheOf(metric).keySet().asScala.exists(a ⇒ a.equals(bucketNumber))
  }

  def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit = {
    if (isEnabledFor(metric) && buckets.length <= Settings.BucketCache.MaximumCacheStore) {
      log.debug(s"Caching ${buckets.length} buckets of ${fromBucketNumber.duration} for $metric")
      val cache = metricCacheOf(metric)
      buckets.foreach { bucket ⇒
        val previousBucket = cache.put(bucket.bucketNumber, bucket)
        if (previousBucket != null) {
          incrementCounter("bucketCache.overrideWarning")
          log.warn("More than one cached Bucket per BucketNumber. Overriding it to leave just one of them.")
        }
      }
      fillEmptyBucketsIfNecessary(cache, fromBucketNumber, toBucketNumber)
    }
  }

  def isEnabledFor(metric: Metric): Boolean = {
    enabled && Settings.BucketCache.IsEnabledFor(metric.mtype)
  }

  def take[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    if (!enabled || !Settings.BucketCache.IsEnabledFor(metric.mtype) || isFirstTimeWindow(fromBucketNumber) ) return None
    val expectedBuckets = toBucketNumber.number - fromBucketNumber.number
    val buckets = takeRecursive(metricCacheOf(metric), fromBucketNumber, toBucketNumber)
    if (buckets.size == expectedBuckets) {
      cacheHit(metric, buckets)
    } else {
      cacheMiss(metric, expectedBuckets)
    }
  }

  def cacheMiss[T <: Bucket](metric: Metric, expectedBuckets: Long): None.type = {
    log.info(s"CacheMiss of ${expectedBuckets} buckets for $metric")
    incrementCounter("bucketCache.miss")
    None
  }

  def cacheHit[T <: Bucket](metric: Metric, buckets: List[(BucketNumber, Any)]): Some[BucketSlice[T]] = {
    log.info(s"CacheHit of ${buckets.size} buckets for $metric")
    incrementCounter("bucketCache.hit")
    val noEmptyBuckets: List[(BucketNumber, Any)] = buckets.filterNot(bucket ⇒ bucket._2.isInstanceOf[EmptyBucket])
    if (noEmptyBuckets.isEmpty) {
      incrementCounter("bucketCache.hit.empty")
    }
    Some(BucketSlice(noEmptyBuckets.map { bucket ⇒
      BucketResult(bucket._1.startTimestamp(), new LazyBucket(bucket._2.asInstanceOf[T]))
    }))
  }

  def isFirstTimeWindow[T <: Bucket](fromBucketNumber: BucketNumber): Boolean = {
    fromBucketNumber.duration == Settings.Window.WindowDurations(0)
  }

  @tailrec
  private def fillEmptyBucketsIfNecessary(cache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber < until) {
      cache.putIfAbsent(bucketNumber, EmptyBucket)
      fillEmptyBucketsIfNecessary(cache, bucketNumber + 1, until)
    }
  }

  private def cleanCache(metric: Metric) = {
    cachesByMetric.remove(metric)
  }

  @tailrec
  private def takeRecursive(metricCache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber, buckets: List[(BucketNumber, Any)] = List[(BucketNumber, Any)]()): List[(BucketNumber, Any)] = {
    if (bucketNumber < until) {
      val bucket = metricCache.remove(bucketNumber)
      takeRecursive(metricCache, bucketNumber + 1, until, if (bucket != null) buckets :+(bucketNumber, bucket) else buckets)
    } else {
      buckets
    }
  }

  private def metricCacheOf(metric: Metric): MetricCache = {
    val metricCache = cachesByMetric.get(metric)
    if (metricCache != null) metricCache
    else {
      val cache: MetricCache = new ConcurrentHashMap()
      val previous = cachesByMetric.putIfAbsent(metric, cache)
      if (previous != null) previous else cache
    }
  }

}

object EmptyBucket extends EmptyBucket

class EmptyBucket extends Bucket(UndefinedBucketNumber) {
  val summary = null
}

object UndefinedBucketNumber extends BucketNumber(-1, null)