package com.despegar.khronus.util

import java.util.concurrent.TimeUnit

import com.despegar.khronus.model.Functions.Percentile95
import com.despegar.khronus.model.{Metric, MetricType, MonitoringSupport, Tick}
import com.despegar.khronus.store.{MetaSupport, Slice, Summaries}
import com.despegar.khronus.util.log.Logging

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait SlowMetricsRecorder extends Logging with MonitoringSupport {

  import SlowMetricsRecorder._

  def measureAndCheckForTimeOutliers[T](label: String, metric: Metric, duration: Duration, debugInfo: ⇒ String = "Empty")(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    if (metric.isSystem) {
      block
    } else {
      val start = now

      block.onSuccess {
        case _ ⇒ {
          val metricKey: String = formatLabel(label, metric, duration)

          val elapsed = now - start
          log.debug(s"${p(metric, duration)} $label - time spent: ${elapsed}ms")
          recordTime(metricKey, elapsed)

          if (enabled) {
            val limit = outliersLimitsCache.putIfAbsent((metricKey, duration), MAX_DEFAULT_OUTLIERS_LIMIT).getOrElse(MAX_DEFAULT_OUTLIERS_LIMIT)
            if (elapsed > limit) {
              log.warn(s"SLOW metric [${metric.name}}][$metricKey] detected: ${elapsed}ms (elapsed) > ${limit}ms (limit). Additional info: $debugInfo")
            }
          }
        }
      }(executionContextOutliers)

      block
    }
  }

  def formatLabel(label: String, metric: Metric, duration: Duration): String = s"$label.${metric.mtype}.${duration.length}${duration.unit}"
}

object SlowMetricsRecorder  extends ConcurrencySupport with MetaSupport {

  private def now = System.currentTimeMillis()

  val executionContextOutliers = executionContext("outliers-worker")

  val outliersLimitsCache: TrieMap[(String, Duration), Long] = TrieMap.empty

  val enabled = Settings.InternalMetrics.CheckOutliers

  //any value that exceed this limit, will be marked as outlier
  val MAX_DEFAULT_OUTLIERS_LIMIT = Tick.smallestWindow().toMillis

  val renewLimitsPool = scheduledThreadPool("outliers-scheduled-worker")
  schedulePool()

  private def schedulePool(): Unit = {
    if (enabled) {
      renewLimitsPool.scheduleAtFixedRate(new Runnable() {

        override def run = renewOutliersLimits
      }, 120, 20, TimeUnit.SECONDS)
    }
  }

  private def renewOutliersLimits: Unit = {
    try {
      outliersLimitsCache.keys foreach { case (metricName, duration) ⇒
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
    case Duration(1, MILLISECONDS) => Tick.smallestWindow().toMillis * 4
    case _ => duration.toMillis * 4
  })

  private def getPercentile(metricName: String, duration: Duration, percentile: Int): Future[Option[Long]] = {
    metaStore.getFromSnapshotSync(metricName) map { case (metric, lastProcess) ⇒
      val slice = Slice(goBack(duration), System.currentTimeMillis())
      val percentile = getStore(metric.mtype).readAll(metric.name, Tick.smallestWindow(), slice, false, 1).map(summaries ⇒
        summaries.headOption map (summary ⇒ summary.get(Percentile95)))(executionContextOutliers)

      percentile
    } getOrElse (Future.successful(Some(MAX_DEFAULT_OUTLIERS_LIMIT)))
  }

  private def getStore(metricType: String) = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ Summaries.histogramSummaryStore
    case MetricType.Counter                  ⇒ Summaries.counterSummaryStore
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }

}
