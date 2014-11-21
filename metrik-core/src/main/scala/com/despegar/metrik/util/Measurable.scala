package com.despegar.metrik.util

import com.despegar.metrik.model.Metric

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Success
import com.despegar.metrik.util.log.Logging

trait Measurable extends Logging {

  private def now = System.currentTimeMillis()

  def measureTime[T](label: String)(block: ⇒ T): T = {
    val start = now
    val blockReturn = block
    log.info(s"$label - time spent: ${now - start}ms")
    blockReturn
  }

  def measureTime[T](label: String, metric: Metric, duration: Duration)(block: ⇒ T): T = {
    measureTime(s"${p(metric, duration)} $label")(block)
  }

  def measureFutureTime[T](label: String, metric: Metric, duration: Duration)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    measureFutureTime(s"${p(metric, duration)} $label")(block)
  }

  def measureFutureTime[T](label: String)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = now
    block andThen {
      case Success(_) ⇒ log.info(s"$label - time spent: ${now - start}ms")
    }
  }
}
