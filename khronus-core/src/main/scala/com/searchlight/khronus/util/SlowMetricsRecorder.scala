package com.searchlight.khronus.util

import java.util.concurrent.TimeUnit

import com.searchlight.khronus.model.Functions.Percentile95
import com.searchlight.khronus.model._
import com.searchlight.khronus.query.Slice
import com.searchlight.khronus.service.MonitoringSupport
import com.searchlight.khronus.store.{ MetaSupport, Summaries }
import com.searchlight.khronus.util.log.Logging

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

trait SlowMetricsRecorder extends Logging with MonitoringSupport {

  import SlowMetricsRecorder._

  def measureAndCheckForTimeOutliers[T](label: String, metric: Metric, duration: Duration, debugInfo: ⇒ String = "Empty")(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    if (metric.isSystem) {
      block
    } else {
      val start = now

      val f = block

      f.onSuccess {
        case _ ⇒ {
          val metricKey: String = formatLabel(label, metric, duration)

          val elapsed = now - start
          log.debug(s"${p(metric, duration)} $label - time spent: ${elapsed}ms")
          recordHistogram(metricKey, elapsed)

          if (enabled) {
            val limit = outliersLimitsCache.putIfAbsent((metricKey, duration), MAX_DEFAULT_OUTLIERS_LIMIT).getOrElse(MAX_DEFAULT_OUTLIERS_LIMIT)
            if (elapsed > limit) {
              log.warn(s"SLOW metric [${metric.flatName}}][$metricKey] detected: ${elapsed}ms (elapsed) > ${limit}ms (limit). Additional info: $debugInfo")
            }
          }
        }
      }(executionContextOutliers)

      f
    }
  }

  def formatLabel(label: String, metric: Metric, duration: Duration): String = s"$label.${metric.mtype}.${duration.length}${duration.unit}"
}

object SlowMetricsRecorder extends ConcurrencySupport with MetaSupport with TimeWindowsSupport {

  private def now = System.currentTimeMillis()

  val executionContextOutliers = executionContext("outliers-worker")

  val outliersLimitsCache: TrieMap[(String, Duration), Long] = TrieMap.empty

  val enabled = Settings.InternalMetrics.CheckOutliers

  val ASCENDING_ORDER = false

  //any value that exceed this limit, will be marked as outlier
  val MAX_DEFAULT_OUTLIERS_LIMIT = smallestWindow.duration.toMillis

  val renewLimitsPool = scheduledThreadPool("outliers-scheduled-worker")
  schedulePool()

  private def schedulePool(): Unit = {
    if (enabled) {
      renewLimitsPool.scheduleAtFixedRate(new Runnable() {
        override def run() = renewOutliersLimits()
      }, 120, 20, TimeUnit.SECONDS)
    }
  }

  private def renewOutliersLimits(): Unit = {
    try {
      outliersLimitsCache.keys foreach {
        case (metricName, duration) ⇒
          getPercentile(s"~system.$metricName", duration, 95).onSuccess {
            case Some(percentile) ⇒ outliersLimitsCache.update((metricName, duration), percentile);
          }(executionContextOutliers)
      }
    } catch {
      case e: Throwable ⇒ log.error("Error on renewOutliersLimits", e)
    }

    log.trace(s"outliersLimitsCache: $outliersLimitsCache")
  }

  private def goBack(duration: Duration): Long = System.currentTimeMillis() - (duration match {
    case Duration(1, MILLISECONDS) ⇒ smallestWindow.duration.toMillis * 4
    case _                         ⇒ duration.toMillis * 4
  })

  private def getPercentile(metricName: String, duration: Duration, percentile: Int): Future[Option[Long]] = {
    metaStore.searchInSnapshotByMetricName(metricName) map {
      case (metric, lastProcess) ⇒
        val slice = Slice(goBack(duration), System.currentTimeMillis())
        val percentile = getStore(metric.mtype).readAll(metric.flatName, smallestWindow.duration, slice, ASCENDING_ORDER, 1).map(summaries ⇒
          summaries.headOption map (summary ⇒ summary.get(Percentile95)))(executionContextOutliers)

        percentile
    } getOrElse Future.successful(Some(MAX_DEFAULT_OUTLIERS_LIMIT))
  }

  private def getStore(metricType: MetricType) = metricType match {
    case Histogram ⇒ Summaries.histogramSummaryStore
    case Counter   ⇒ Summaries.counterSummaryStore
    case Gauge     ⇒ Summaries.gaugeSummaryStore
  }

}
