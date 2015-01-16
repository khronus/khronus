package com.despegar.khronus.store

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.despegar.khronus.model._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ Measurable, Settings }

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait BucketCacheSupport {

  val bucketCache: BucketCache = InMemoryBucketCache

}

trait BucketCache {

  def markProcessedTick(metric: Metric, tick: Tick): Unit

  def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit

  def take[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[Seq[(Timestamp, () ⇒ T)]]

}

object InMemoryBucketCache extends BucketCache with Logging with Measurable {

  type MetricCache = ConcurrentHashMap[BucketNumber, Any]
  private val cachedBuckets = new ConcurrentHashMap[Metric, MetricCache]()
  private val lastKnownTick = new AtomicReference[Tick]()

  private val enabled = Settings.BucketCache.Enabled

  log.info(s"BucketCache is ${if (enabled) "enabled" else "disabled"}")

  def markProcessedTick(metric: Metric, tick: Tick) = if (enabled) {
    val previousKnownTick = lastKnownTick.getAndSet(tick)
    if (previousKnownTick != tick && previousKnownTick != null) {
      cachedBuckets.keySet().asScala.foreach { metric ⇒
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

  def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit = if (enabled && Settings.BucketCache.IsEnabledFor(metric.mtype)) {
    if ((toBucketNumber.number - fromBucketNumber.number) > Settings.BucketCache.MaximumCacheStore) {
      return ;
    }
    log.info(s"Caching ${(toBucketNumber.number - fromBucketNumber.number)} buckets of ${fromBucketNumber.duration} for $metric")
    val cache = metricCacheOf(metric)
    buckets.foreach { bucket ⇒
      val value: Any = if (metric.mtype.equals(MetricType.Timer) || metric.mtype.equals(MetricType.Gauge)) {
        HistogramSerializer.serialize(bucket.asInstanceOf[HistogramBucket].histogram)
      } else {
        bucket
      }
      val previousBucket = cache.putIfAbsent(bucket.bucketNumber, value)
      if (previousBucket != null) {
        incrementCounter("bucketCache.overrideWarning")
        log.warn("More than one cached Bucket per BucketNumber. Overriding it to leave just one of them.")
        cache.put(bucket.bucketNumber, value)
      }
    }
    fillEmptyBucketsIfNecessary(cache, fromBucketNumber, toBucketNumber)
  }

  def take[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[Seq[(Timestamp, () ⇒ T)]] = {
    if (!enabled || fromBucketNumber.duration == Settings.Window.WindowDurations(0) || !Settings.BucketCache.IsEnabledFor(metric.mtype)) return None
    val buckets = takeRecursive(metricCacheOf(metric), fromBucketNumber, toBucketNumber)
    val expectedBuckets = toBucketNumber.number - fromBucketNumber.number
    if (buckets.size == expectedBuckets) {
      log.info(s"CacheHit of ${buckets.size} buckets for $metric")
      incrementCounter("bucketCache.hit")
      val noEmptyBuckets: List[(BucketNumber, Any)] = buckets.filterNot(bucket ⇒ bucket._2.isInstanceOf[EmptyBucket])
      if (noEmptyBuckets.isEmpty) {
        incrementCounter("bucketCache.hitEmpty")
      }
      Some(noEmptyBuckets.map { bucket ⇒
        (bucket._1.startTimestamp(), () ⇒ {
          if (metric.mtype.equals(MetricType.Timer) || metric.mtype.equals(MetricType.Gauge)) {
            new HistogramBucket(bucket._1, HistogramSerializer.deserialize(bucket._2.asInstanceOf[Array[Byte]])).asInstanceOf[T]
          } else {
            bucket._2.asInstanceOf[T]
          }
        })
      })
    } else {
      log.info(s"CacheMiss of ${expectedBuckets} buckets for $metric")
      incrementCounter("bucketCache.miss")
      None
    }
  }

  @tailrec
  private def fillEmptyBucketsIfNecessary(cache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber < until) {
      cache.putIfAbsent(bucketNumber, EmptyBucket)
      fillEmptyBucketsIfNecessary(cache, bucketNumber + 1, until)
    }
  }

  private def cleanCache(metric: Metric) = {
    cachedBuckets.remove(metric)
  }

  @tailrec
  private def takeRecursive(metricCache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber, buckets: List[(BucketNumber, Any)] = List[(BucketNumber, Any)]()): List[(BucketNumber, Any)] = {
    if (bucketNumber < until) {
      val bucket = metricCache.remove(bucketNumber)
      takeRecursive(metricCache, bucketNumber + 1, until, if (bucket != null) buckets :+ (bucketNumber, bucket) else buckets)
    } else {
      buckets
    }
  }

  private def metricCacheOf(metric: Metric): MetricCache = {
    val metricCache = cachedBuckets.get(metric)
    if (metricCache != null) metricCache
    else {
      val cache: MetricCache = new ConcurrentHashMap()
      val previous = cachedBuckets.putIfAbsent(metric, cache)
      if (previous != null) previous else cache
    }
  }

}

object EmptyBucket extends EmptyBucket

class EmptyBucket extends Bucket(UndefinedBucketNumber) {
  val summary = null
}

object UndefinedBucketNumber extends BucketNumber(-1, null)