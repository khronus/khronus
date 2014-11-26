package com.despegar.metrik.util

import com.despegar.metrik.model.{Metric, MonitoringSupport}
import com.despegar.metrik.util.log.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait Measurable extends Logging with MonitoringSupport {

  private def now = System.currentTimeMillis()

  private def measureTime[T](label: String, text: String)(block: ⇒ T): T = {
    val start = now
    val blockReturn = block
    val elapsed = now - start
    log.info(s"$text - time spent: ${elapsed}ms")
    recordTime(label, elapsed)
    blockReturn
  }

  def measureTime[T](label: String, metric: Metric, duration: Duration)(block: ⇒ T): T = {
    if (!metric.isSystem) {
      measureTime(formatLabel(label, metric, duration), s"${p(metric, duration)} $label")(block)
    } else {
      block
    }
  }

  def measureFutureTime[T](label: String, metric: Metric, duration: Duration)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    if (!metric.isSystem) {
      measureFutureTime(formatLabel(label,metric,duration),  s"${p(metric, duration)} $label")(block)
    } else {
      block
    }
  }

  def measureFutureTime[T](label: String, text: String)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = now
    block andThen {
      case Success(_) ⇒ {
        val elapsed = now - start
        log.info(s"$text - time spent: ${elapsed}ms")
        recordTime(label, elapsed)
      }
    }
  }

  private def formatLabel(label: String, metric: Metric, duration: Duration): String = s"$label.${metric.mtype}.${duration.length}${duration.unit}"
}
