package com.despegar.khronus.store

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

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

  def multiSet(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit

  def multiGet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]]

}

object InMemoryBucketCache extends BucketCache with Logging with Measurable {

  type MetricCache = ConcurrentHashMap[BucketNumber, Bucket]
  private val cachedMetrics = new AtomicLong(0)
  private val cachesByMetric = new ConcurrentHashMap[Metric, MetricBucketCache[_ <: Bucket]]()
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
    !metricCacheOf(metric).map(cache => cache.keySet().asScala.exists(a ⇒ a.contains(bucketNumber))).getOrElse(false)
  }

  def multiSet(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]): Unit = {
    if (isEnabledFor(metric) && (toBucketNumber.number - fromBucketNumber.number - 1) <= Settings.BucketCache.MaxStore) {
      log.debug(s"Caching ${buckets.length} buckets of ${fromBucketNumber.duration} for $metric")
      metricCacheOf(metric).map { cache =>
        buckets.foreach { bucket ⇒
          val previousBucket: Bucket = setBucket(cache, bucket)
          if (previousBucket != null) {
            incrementCounter("bucketCache.overrideWarning")
            log.warn("More than one cached Bucket per BucketNumber. Overriding it to leave just one of them.")
          }
        }
        fillEmptyBucketsIfNecessary(metric, cache, fromBucketNumber, toBucketNumber)
      }
    }
  }

  def isEnabledFor(metric: Metric): Boolean = {
    enabled && Settings.BucketCache.IsEnabledFor(metric.mtype)
  }

  def multiGet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    if (!enabled || !Settings.BucketCache.IsEnabledFor(metric.mtype) || isRawTimeWindow(fromBucketNumber)) return None
    val expectedBuckets = toBucketNumber.number - fromBucketNumber.number
    val slice:Option[BucketSlice[T]] = metricCacheOf(metric).flatMap { cache =>
      val buckets = takeRecursive(cache, fromBucketNumber, toBucketNumber)
      if (buckets.size == expectedBuckets) {
        cacheHit(metric, buckets, fromBucketNumber, toBucketNumber)
      } else {
        None
      }
    }
    if (slice.isEmpty) {
      cacheMiss(metric, expectedBuckets, fromBucketNumber, toBucketNumber)
    }
    slice
  }

  def cacheMiss[T <: Bucket](metric: Metric, expectedBuckets: Long, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    log.debug(s"CacheMiss of ${expectedBuckets} buckets for $metric between $fromBucketNumber and $toBucketNumber")
    incrementCounter("bucketCache.miss")
    None
  }

  def cacheHit[T <: Bucket](metric: Metric, buckets: List[(BucketNumber, Any)], fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    log.debug(s"CacheHit of ${buckets.size} buckets for $metric between $fromBucketNumber and $toBucketNumber")
    incrementCounter("bucketCache.hit")
    val noEmptyBuckets: List[(BucketNumber, Any)] = buckets.filterNot(bucket ⇒ bucket._2.isInstanceOf[EmptyBucket])
    if (noEmptyBuckets.isEmpty) {
      incrementCounter("bucketCache.hit.empty")
    }
    Some(BucketSlice(noEmptyBuckets.map { bucket ⇒
      BucketResult(bucket._1.startTimestamp(), new LazyBucket(bucket._2.asInstanceOf[T]))
    }))
  }

  def isRawTimeWindow[T <: Bucket](fromBucketNumber: BucketNumber): Boolean = {
    fromBucketNumber.duration == Settings.Window.RawDuration
  }

  @tailrec
  private def fillEmptyBucketsIfNecessary(metric: Metric, cache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber < until) {
      val previous = cache.putIfAbsent(bucketNumber, EmptyBucket)
      if (previous == null) {
        log.debug(s"Filling empty bucket $bucketNumber of metric $metric")
      }
      fillEmptyBucketsIfNecessary(metric, cache, bucketNumber + 1, until)
    }
  }

  private def cleanCache(metric: Metric) = {
    log.debug(s"Lose $metric affinity. Cleaning its bucket cache")
    cachesByMetric.remove(metric)
    cachedMetrics.decrementAndGet()
  }

  @tailrec
  private def takeRecursive(metricCache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber, buckets: List[(BucketNumber, Any)] = List[(BucketNumber, Any)]()): List[(BucketNumber, Any)] = {
    if (bucketNumber < until) {
      val bucket: Bucket = getBucket(metricCache, bucketNumber)
      takeRecursive(metricCache, bucketNumber + 1, until, if (bucket != null) buckets :+(bucketNumber, bucket) else buckets)
    } else {
      buckets
    }
  }

  def getBucket(metricCache: MetricCache, bucketNumber: BucketNumber): Bucket = {
    val bucket = metricCache.remove(bucketNumber)
    bucket
  }

  def setBucket(cache: MetricCache, bucket: Bucket): Bucket = {
    val previousBucket = cache.put(bucket.bucketNumber, bucket)
    previousBucket
  }

  private def metricCacheOf(metric: Metric): Option[MetricBucketCache[_ <: Bucket]] = {
    val metricCache = cachesByMetric.get(metric)
    if (metricCache != null) Some(metricCache)
    else {
      if (cachedMetrics.incrementAndGet() > Settings.BucketCache.MaxMetrics) {
        cachedMetrics.decrementAndGet()
        None
      } else {

        val cache: MetricCache = new ConcurrentHashMap()
        val previous = cachesByMetric.putIfAbsent(metric, cache)
        Some(if (previous != null) previous else cache)
      }
    }
  }

}


trait MetricBucketCache[T <: Bucket] {
  protected val cache = new ConcurrentHashMap[BucketNumber, T]()
  def get(bucketNumber: BucketNumber): T
  def set(bucketNumber: BucketNumber, bucket: T)
}

class CounterMetricBucketCache extends MetricBucketCache[CounterBucket] {
  override def get(bucketNumber: BucketNumber): CounterBucket = cache.get(bucketNumber)

  override def set(bucketNumber: BucketNumber, bucket: CounterBucket): Unit = cache.put(bucketNumber, bucket)
}

class HistogramMetricBucketCache extends MetricBucketCache[HistogramBucket] {
  override def get(bucketNumber: BucketNumber): HistogramBucket = cache.get(bucketNumber)

  override def set(bucketNumber: BucketNumber, bucket: HistogramBucket): Unit = cache.put(bucketNumber, bucket)
}

object EmptyBucket extends EmptyBucket

class EmptyBucket extends Bucket(UndefinedBucketNumber) {
  val summary = null
}

object UndefinedBucketNumber extends BucketNumber(-1, null) {
  override def toString = {
    "UndefinedBucketNumber"
  }
}