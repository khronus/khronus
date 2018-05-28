package com.searchlight.khronus.influx.parser

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.searchlight.khronus.model.{Metric, Monitoring}
import com.searchlight.khronus.store.MetaSupport
import com.searchlight.khronus.util.ConcurrencySupport


trait InfluxParserCache {

  def getMetricFromCache(regex: String): Seq[Metric] = {
    val value = InfluxParserCache.cacheMetrics.get(regex)
    if (value == null)
      Seq.empty[Metric]
    else
      value
  }
}

object InfluxParserCache extends MetaSupport with ConcurrencySupport {

  val cacheMetrics: LoadingCache[String, Seq[Metric]] = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .expireAfterAccess(1, TimeUnit.DAYS)
    .recordStats().
    build(new CacheLoader[String, Seq[Metric]] {
      override def load(key: String): Seq[Metric] = {
        metaStore.searchInSnapshotByRegex(getCaseInsensitiveRegex(key))
      }
    })


  def getCaseInsensitiveRegex(metricNameRegex: String) = {
    val caseInsensitiveRegex = "(?i)"
    s"$caseInsensitiveRegex$metricNameRegex"
  }

  def logCacheStats() = {
    Monitoring.incrementCounter(s"parserCache.evictions", cacheMetrics.stats().evictionCount())
    Monitoring.incrementCounter(s"parserCache.hitCount", cacheMetrics.stats().hitCount())
    Monitoring.incrementCounter(s"parserCache.missCount", cacheMetrics.stats().missCount())
    Monitoring.incrementCounter(s"parserCache.size", cacheMetrics.size())
  }

  private val scheduledCachePool = scheduledThreadPool(s"parser-cache-scheduled-worker")
  scheduledCachePool.scheduleAtFixedRate(new Runnable() { override def run() = logCacheStats() }, 0, 10, TimeUnit.SECONDS)

}