package com.despegar.khronus.util

import java.util.concurrent.TimeUnit

import com.despegar.khronus.model.MetricType
import com.despegar.khronus.store.{Slice, Summaries, MetaSupport}
import com.despegar.khronus.util.log.Logging

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait SlowMetricsRecorder extends ConcurrencySupport with Logging with MetaSupport {

  private def now = System.currentTimeMillis()

  val executionContextOutliers = executionContext("outliers-worker")

  val renewLimitsPool = scheduledThreadPool("outliers-scheduled-worker")
  renewLimitsPool.scheduleAtFixedRate(new Runnable() {


    override def run = renewOutliersLimits
  }, 120, 10, TimeUnit.SECONDS)

  val outliersLimitsCache: TrieMap[String, Long] = TrieMap.empty

  //any value that exceed this limit, will be marked as outlier
  val MAX_DEFAULT_OUTLIERS_LIMIT = (30 seconds).toMillis

  val DEFAULT_REFERENCE_TIME_WINDOW = 30 seconds

  def checkForSlowQuery[T](metricName: String, query: => String)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = now
    val f = block

    f.onSuccess {
      case _ ⇒ {
        val limit = outliersLimitsCache.putIfAbsent(metricName, MAX_DEFAULT_OUTLIERS_LIMIT).getOrElse(MAX_DEFAULT_OUTLIERS_LIMIT)
        val elapsed = now - start
        if (elapsed > limit) {
          log.warn(s"SLOW query detected: $elapsed (elapsed) > $limit (limit). Query: $query")
        }
      }
    }(executionContextOutliers)

    f
  }


  private def renewOutliersLimits: Unit = {
    try {
      outliersLimitsCache.keys foreach { metric =>
        val p95: Long = getPercentile(metric, 95)
        outliersLimitsCache.update(metric, p95)
      }
    } catch {
      case e: Throwable => log.error("Error on renewOutliersLimits", e)
    }
  }

  private def getPercentile(metricName: String, percentile: Int): Long = {
    metaStore.searchInSnapshotSync(metricName).headOption map { metric =>
      val slice = Slice(System.currentTimeMillis() - ((DEFAULT_REFERENCE_TIME_WINDOW).toMillis * 2), System.currentTimeMillis())
      val summaries = getStore(metric.mtype).readAll(metric.name, DEFAULT_REFERENCE_TIME_WINDOW, slice, true, 1)
    }

    MAX_DEFAULT_OUTLIERS_LIMIT
  }

  private def getStore(metricType: String) = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ Summaries.histogramSummaryStore
    case MetricType.Counter                  ⇒ Summaries.counterSummaryStore
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }
}
