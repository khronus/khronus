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

  def markProcessedTick(metric: Metric, tick: Tick) = {
    val previousKnownTick = lastKnownTick.getAndSet(tick)
    if (previousKnownTick != tick) {
      cachedBuckets.keySet().asScala.foreach { metric ⇒
        if (noCachedBucketFor(metric, previousKnownTick.bucketNumber)) {
          incrementCounter("bucketCache.noMetricAffinity")
          cleanCache(metric)
        }
      }
    }
  }

  private def noCachedBucketFor(metric: Metric, bucketNumber: BucketNumber): Boolean = {
    !metricCacheOf(metric).keySet().asScala.exists(_.equals(bucketNumber))
  }

  def cacheBuckets(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[Bucket]) = {
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
    if (fromBucketNumber.duration == Settings.Histogram.TimeWindows(0).duration) return None
    val buckets = takeRecursive(metricCacheOf(metric), fromBucketNumber, toBucketNumber)
    val expectedBuckets = toBucketNumber.number - fromBucketNumber.number
    if (buckets.size == expectedBuckets) {
      incrementCounter("bucketCache.hit")
      val noEmptyBuckets: List[(BucketNumber, Any)] = buckets.filterNot(bucket ⇒ bucket._2.isInstanceOf[EmptyBucket])
      if (noEmptyBuckets.isEmpty) {
        incrementCounter("bucketCache.hitEmpty")
      }
      Some(noEmptyBuckets.map { bucket ⇒
        (bucket._1.startTimestamp(), () ⇒ {
          if (metric.mtype.equals(MetricType.Timer) || metric.mtype.equals(MetricType.Gauge)) {
            HistogramSerializer.deserialize(bucket._2.asInstanceOf[Array[Byte]]).asInstanceOf[T]
          } else {
            bucket._2.asInstanceOf[T]
          }
        })
      })
    } else {
      incrementCounter("bucketCache.miss")
      None
    }
  }

  @tailrec
  private def fillEmptyBucketsIfNecessary(cache: MetricCache, bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber <= until) {
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
    if (metricCache != null) metricCache else cachedBuckets.putIfAbsent(metric, new MetricCache())
  }

}

object EmptyBucket extends EmptyBucket

class EmptyBucket extends Bucket(UndefinedBucketNumber) {
  val summary = null
}

object UndefinedBucketNumber extends BucketNumber(-1, null)