package com.despegar.metrik.util

import com.despegar.metrik.model.Metric

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.util.Success

trait Measurable extends Logging {

  def measureTime[T](label: String, metric: Metric, duration: Duration)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    measureTime(s"${p(metric, duration)} $label")(block)
  }

  def measureTime[T](label: String)(block: ⇒ Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val start = System.currentTimeMillis()
    block andThen {
      case Success(_) ⇒ log.info(s"$label - Processed in ${System.currentTimeMillis() - start}ms")
    }
  }
}
