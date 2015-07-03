package com.despegar.khronus.store

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

import com.despegar.khronus.model._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ Measurable, Settings }

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

trait BucketCacheSupport {
  val bucketCache: BucketCache = InMemoryBucketCache
}

trait BucketCache {

  def markProcessedTick(metric: Metric, tick: Tick): Unit

  def multiSet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[T]): Unit

  def multiGet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]]

}

object InMemoryBucketCache extends BucketCache with Logging with Measurable {
  private val cachesByMetric: TrieMap[Metric, MetricBucketCache[_ <: Bucket]] = new TrieMap[Metric, MetricBucketCache[_ <: Bucket]]()
  private val nCachedMetrics = new AtomicLong(0)
  private val lastKnownTick = new AtomicReference[Tick]()

  private val enabled = Settings.BucketCache.Enabled

  override def markProcessedTick(metric: Metric, tick: Tick) = if (enabled) {
    val previousKnownTick = lastKnownTick.getAndSet(tick)
    if (previousKnownTick != tick && previousKnownTick != null) {
      cachesByMetric.keySet.foreach { metric ⇒
        if (noCachedBucketFor(metric, previousKnownTick.bucketNumber)) {
          incrementCounter("bucketCache.noMetricAffinity")
          cleanCache(metric)
        }
      }
    }
  }

  override def multiGet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    if (!enabled || !Settings.BucketCache.IsEnabledFor(metric.mtype) || isRawTimeWindow(fromBucketNumber)) return None
    val expectedBuckets = toBucketNumber.number - fromBucketNumber.number
    val slice: Option[BucketSlice[T]] = metricCacheOf(metric).flatMap { cache ⇒
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

  override def multiSet[T <: Bucket](metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[T]): Unit = {
    if (isEnabledFor(metric) && (toBucketNumber.number - fromBucketNumber.number - 1) <= Settings.BucketCache.MaxStore) {
      log.debug(s"Caching ${buckets.length} buckets of ${fromBucketNumber.duration} for $metric")
      metricCacheOf(metric).map { cache ⇒
        buckets.foreach { bucket ⇒
          val previousBucket = cache.putIfAbsent(bucket.bucketNumber, bucket)
          if (previousBucket != null) {
            incrementCounter("bucketCache.overrideWarning")
            log.warn("More than one cached Bucket per BucketNumber. Overriding it to leave just one of them.")
          }
        }
        fillEmptyBucketsIfNecessary(metric, cache, fromBucketNumber, toBucketNumber)
      }
    }
  }

  private def cacheMiss[T <: Bucket](metric: Metric, expectedBuckets: Long, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    log.debug(s"CacheMiss of ${expectedBuckets} buckets for $metric between $fromBucketNumber and $toBucketNumber")
    incrementCounter("bucketCache.miss")
    None
  }

  private def cacheHit[T <: Bucket](metric: Metric, buckets: List[(BucketNumber, Any)], fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
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

  private def isRawTimeWindow[T <: Bucket](fromBucketNumber: BucketNumber): Boolean = {
    fromBucketNumber.duration == Settings.Window.RawDuration
  }

  private def isEnabledFor(metric: Metric): Boolean = {
    enabled && Settings.BucketCache.IsEnabledFor(metric.mtype)
  }

  private def metricCacheOf(metric: Metric): Option[MetricBucketCache[_ <: Bucket]] = {
    val cache = cachesByMetric.get(metric)
    if (cache.isDefined) {
      cache
    } else {
      if (nCachedMetrics.incrementAndGet() > Settings.BucketCache.MaxMetrics) {
        nCachedMetrics.decrementAndGet()
        None
      } else {
        val cache = metric.mtype match {
          case MetricType.Counter ⇒ new CounterMetricBucketCache()
          case MetricType.Timer   ⇒ new HistogramMetricBucketCache()
          case MetricType.Gauge   ⇒ new HistogramMetricBucketCache()
        }

        val previous = cachesByMetric.putIfAbsent(metric, cache)

        if (previous != null) previous else Some(cache)
      }
    }
  }

  @tailrec
  def fillEmptyBucketsIfNecessary(metric: Metric, cache: MetricBucketCache[_ <: Bucket], bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber < until) {
      val previous = cache.putIfAbsent(bucketNumber, new EmptyBucket())
      if (previous == null) {
        log.debug(s"Filling empty bucket $bucketNumber of metric $metric")
      }
      fillEmptyBucketsIfNecessary(metric, cache, bucketNumber + 1, until)
    }
  }

  @tailrec
  private def takeRecursive(metricCache: MetricBucketCache[_ <: Bucket], bucketNumber: BucketNumber, until: BucketNumber, buckets: List[(BucketNumber, Any)] = List[(BucketNumber, Any)]()): List[(BucketNumber, Any)] = {
    if (bucketNumber < until) {
      val bucket: Bucket = metricCache.remove(bucketNumber)
      takeRecursive(metricCache, bucketNumber + 1, until, if (bucket != null) buckets :+ (bucketNumber, bucket) else buckets)
    } else {
      buckets
    }
  }

  private def cleanCache(metric: Metric) = {
    log.debug(s"Lose $metric affinity. Cleaning its bucket cache")
    cachesByMetric.remove(metric)
    nCachedMetrics.decrementAndGet()
  }

  private def noCachedBucketFor(metric: Metric, bucketNumber: BucketNumber): Boolean = {
    !metricCacheOf(metric).map(cache ⇒ cache.keySet().exists(a ⇒ a.contains(bucketNumber))).getOrElse(false)
  }
}

trait MetricBucketCache[T] {
  protected val cache = new ConcurrentHashMap[BucketNumber, Array[Byte]]()

  def serialize[T <: Bucket](bucket: T): Array[Byte]

  def deserialize[T <: Bucket](bytes: Array[Byte]): T

  def putIfAbsent[T <: Bucket](bucketNumber: BucketNumber, bucket: T): Bucket = {
    if (bucket.isInstanceOf[EmptyBucket]) {
      val older = cache.putIfAbsent(bucketNumber, Array.empty[Byte])
      checkEmptyBucket(older)
    } else {
      deserialize(cache.putIfAbsent(bucketNumber, serialize(bucket)))
    }
  }

  def remove(bucketNumber: BucketNumber): Bucket = {
    val bytes = cache.remove(bucketNumber)
    checkEmptyBucket(bytes)
  }

  def checkEmptyBucket(bytes: Array[Byte]): Bucket = {
    if (bytes.length == 0) {
      EmptyBucket
    } else {
      deserialize(bytes)
    }
  }

  def keySet(): mutable.Set[BucketNumber] = cache.keySet().asScala
}

class CounterMetricBucketCache extends MetricBucketCache[CounterBucket] {
  override def serialize[T <: CounterBucket](bucket: T): Array[Byte] = ???

  override def deserialize[T <: CounterBucket](bytes: Array[Byte]): T = ???
}

class HistogramMetricBucketCache extends MetricBucketCache[HistogramBucket] {
  override def serialize[T <: HistogramBucket](bucket: T): Array[Byte] = ???

  override def deserialize[T <: HistogramBucket](bytes: Array[Byte]): T = ???
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
