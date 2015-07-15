package com.despegar.khronus.store

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

import com.despegar.khronus.model._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ Measurable, Settings }

import scala.annotation.tailrec
import scala.collection.Set
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

trait BucketCacheSupport[T <: Bucket] {
  val bucketCache: BucketCache[T]
}

trait BucketCache[T <: Bucket] extends Logging with Measurable {
  val cachesByMetric: TrieMap[Metric, MetricBucketCache[T]]
  val nCachedMetrics: Map[String, AtomicLong]
  private val enabled = Settings.BucketCache.Enabled

  val globalLastKnownTick: AtomicReference[Tick] = new AtomicReference[Tick]()

  def markProcessedTick(tick: Tick): Unit = if (enabled) {
    //gobalLastKnownTick for only one do the check affinity for all metrics
    val globalPreviousKnownTick = globalLastKnownTick.getAndSet(tick)
    if (globalPreviousKnownTick != tick) {
      cachesByMetric map {
        case (metric, cache) ⇒
          //Ticks must be consecutive to ensure affinity
          val metricPreviousTick = cache.lastKnownTick.getAndSet(tick)
          if (metricPreviousTick != null && !metricPreviousTick.bucketNumber.following.equals(tick.bucketNumber)) {
            incrementCounter("bucketCache.noMetricAffinity")
            cleanCache(metric)
          }
      }
    }

    nCachedMetrics foreach { case (mtype, counter) ⇒ recordGauge(s"bucketCache.size.$mtype", counter.get()) }
  }

  def multiSet(metric: Metric, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber, buckets: Seq[T]): Unit = {
    if (isEnabledFor(metric) && (toBucketNumber.number - fromBucketNumber.number - 1) <= Settings.BucketCache.MaxStore) {
      log.debug(s"Caching ${buckets.length} buckets of ${fromBucketNumber.duration} for $metric")
      metricCacheOf(metric).map { cache ⇒
        buckets.foreach { bucket ⇒
          cache.putIfAbsent(bucket.bucketNumber, bucket) map { _ ⇒
            incrementCounter("bucketCache.overrideWarning")
            log.warn("More than one cached Bucket per BucketNumber. Overriding it to leave just one of them.")
          }
        }
        fillEmptyBucketsIfNecessary(metric, cache, fromBucketNumber, toBucketNumber)
      }
    }
  }

  def sliceExceeded(currentDuration: Duration, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Boolean = {
    val diff = toBucketNumber.number - fromBucketNumber.number
    //how many buckets we need to fill the current duration. Ex: 5 minute window require 5 buckets of 1 minute
    val previousDurationBuckets = currentDuration / fromBucketNumber.duration
    val exceeded = diff > (previousDurationBuckets.toLong * Settings.BucketCache.MaxStore)

    if (exceeded) {
      log.debug(s"Exceeded max slice in cache multiget. From $fromBucketNumber to $toBucketNumber")
      incrementCounter("bucketCache.sliceExceeded")
    }

    exceeded
  }

  def multiGet(metric: Metric, currentDuration: Duration, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    if (!enabled || !Settings.BucketCache.IsEnabledFor(metric.mtype) || isRawTimeWindow(fromBucketNumber) || sliceExceeded(currentDuration, fromBucketNumber, toBucketNumber)) return None
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

  private def cleanCache(metric: Metric) = {
    log.debug(s"Lose $metric affinity. Cleaning its bucket cache")
    cachesByMetric.remove(metric)
    nCachedMetrics(metric.mtype).decrementAndGet()
  }

  private def metricCacheOf(metric: Metric): Option[MetricBucketCache[T]] = {
    val currentCache = cachesByMetric.get(metric)
    if (currentCache.isDefined) {
      currentCache
    } else {
      if (nCachedMetrics(metric.mtype).incrementAndGet() > Settings.BucketCache.MaxMetrics(metric.mtype)) {
        nCachedMetrics(metric.mtype).decrementAndGet()
        incrementCounter(s"bucketCache.maxMetrics.${metric.mtype}")
        None
      } else {
        val newCache = buildCache()
        val previous = cachesByMetric.putIfAbsent(metric, newCache)

        if (previous.isDefined) previous else Option(newCache)
      }
    }
  }

  def buildCache(): MetricBucketCache[T]

  private def isEnabledFor(metric: Metric): Boolean = {
    enabled && Settings.BucketCache.IsEnabledFor(metric.mtype)
  }

  @tailrec
  private def fillEmptyBucketsIfNecessary(metric: Metric, cache: MetricBucketCache[T], bucketNumber: BucketNumber, until: BucketNumber): Unit = {
    if (bucketNumber < until) {
      val previous = cache.putIfAbsent(bucketNumber, cache.buildEmptyBucket())
      if (previous.isEmpty) {
        log.debug(s"Filling empty bucket $bucketNumber of metric $metric")
      }
      fillEmptyBucketsIfNecessary(metric, cache, bucketNumber + 1, until)
    }
  }

  @tailrec
  private def takeRecursive(metricCache: MetricBucketCache[T], bucketNumber: BucketNumber, until: BucketNumber, buckets: List[(BucketNumber, Bucket)] = List[(BucketNumber, Bucket)]()): List[(BucketNumber, Bucket)] = {
    if (bucketNumber < until) {
      val bucket: Option[Bucket] = metricCache.remove(bucketNumber)
      takeRecursive(metricCache, bucketNumber + 1, until, if (bucket.isDefined) buckets :+ (bucketNumber, bucket.get) else buckets)
    } else {
      buckets
    }
  }

  private def isRawTimeWindow(fromBucketNumber: BucketNumber): Boolean = {
    fromBucketNumber.duration == Settings.Window.RawDuration
  }

  private def cacheMiss(metric: Metric, expectedBuckets: Long, fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
    log.debug(s"CacheMiss of ${expectedBuckets} buckets for $metric between $fromBucketNumber and $toBucketNumber")
    incrementCounter("bucketCache.miss")
    None
  }

  private def cacheHit(metric: Metric, buckets: List[(BucketNumber, Any)], fromBucketNumber: BucketNumber, toBucketNumber: BucketNumber): Option[BucketSlice[T]] = {
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

}

object InMemoryCounterBucketCache extends BucketCache[CounterBucket] {
  override val cachesByMetric: TrieMap[Metric, MetricBucketCache[CounterBucket]] = new TrieMap[Metric, MetricBucketCache[CounterBucket]]()
  override val nCachedMetrics: Map[String, AtomicLong] = Map(MetricType.Counter -> new AtomicLong(0))

  override def buildCache(): MetricBucketCache[CounterBucket] = new CounterMetricBucketCache()

}

object InMemoryHistogramBucketCache extends BucketCache[HistogramBucket] {
  override val cachesByMetric: TrieMap[Metric, MetricBucketCache[HistogramBucket]] = new TrieMap[Metric, MetricBucketCache[HistogramBucket]]()
  override val nCachedMetrics: Map[String, AtomicLong] = Map(MetricType.Gauge -> new AtomicLong(0), MetricType.Timer -> new AtomicLong(0))

  override def buildCache(): MetricBucketCache[HistogramBucket] = new HistogramMetricBucketCache()
}

trait MetricBucketCache[T <: Bucket] {
  def buildEmptyBucket(): T

  protected val cache = new TrieMap[BucketNumber, Array[Byte]]()

  val lastKnownTick: AtomicReference[Tick] = new AtomicReference[Tick]()

  def serialize(bucket: T): Array[Byte]

  def deserialize(bytes: Array[Byte], bucketNumber: BucketNumber): T

  def putIfAbsent(bucketNumber: BucketNumber, bucket: T): Option[Bucket] = {
    if (bucket.isInstanceOf[EmptyBucket]) {
      cache.putIfAbsent(bucketNumber, Array.empty[Byte]) map (older ⇒ checkEmptyBucket(older, bucketNumber))
    } else {
      cache.putIfAbsent(bucketNumber, serialize(bucket)) map (older ⇒ deserialize(older, bucketNumber))
    }
  }

  def remove(bucketNumber: BucketNumber): Option[Bucket] = {
    cache.remove(bucketNumber) map (bytes ⇒ checkEmptyBucket(bytes, bucketNumber))
  }

  def checkEmptyBucket(bytes: Array[Byte], bucketNumber: BucketNumber): Bucket = {
    if (bytes.length == 0) {
      buildEmptyBucket()
    } else {
      deserialize(bytes, bucketNumber)
    }
  }

  def keySet(): Set[BucketNumber] = cache.keySet
}

class CounterMetricBucketCache extends MetricBucketCache[CounterBucket] {
  private val serializer: CounterBucketSerializer = DefaultCounterBucketSerializer

  override def serialize(bucket: CounterBucket): Array[Byte] = serializer.serialize(bucket).array()

  override def deserialize(bytes: Array[Byte], bucketNumber: BucketNumber): CounterBucket = new CounterBucket(bucketNumber, serializer.deserializeCounts(bytes))

  override def buildEmptyBucket(): CounterBucket = EmptyCounterBucket
}

class HistogramMetricBucketCache extends MetricBucketCache[HistogramBucket] {
  private val histogramSerializer: HistogramSerializer = DefaultHistogramSerializer

  override def serialize(bucket: HistogramBucket): Array[Byte] = histogramSerializer.serialize(bucket.histogram).array()

  override def deserialize(bytes: Array[Byte], bucketNumber: BucketNumber): HistogramBucket = new HistogramBucket(bucketNumber, histogramSerializer.deserialize(ByteBuffer.wrap(bytes)))

  override def buildEmptyBucket(): HistogramBucket = EmptyHistogramBucket
}

trait EmptyBucket

object EmptyHistogramBucket extends EmptyHistogramBucket

class EmptyHistogramBucket extends HistogramBucket(UndefinedBucketNumber, null) with EmptyBucket {
  override val summary = null
}

object EmptyCounterBucket extends EmptyCounterBucket

class EmptyCounterBucket extends CounterBucket(UndefinedBucketNumber, 0) with EmptyBucket {
  override val summary = null
}

object UndefinedBucketNumber extends BucketNumber(-1, null) {
  override def toString() = "UndefinedBucketNumber"
}