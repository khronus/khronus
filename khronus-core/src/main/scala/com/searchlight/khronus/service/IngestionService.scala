package com.searchlight.khronus.service

import com.searchlight.khronus.dao.MetricMeasurementStore
import com.searchlight.khronus.model.MetricMeasurement
import com.searchlight.khronus.util.ConcurrencySupport

import scala.concurrent.{ ExecutionContext, Future }

case class IngestionService() extends MetricMeasurementStore.Support with ConcurrencySupport {
  implicit val executionContext: ExecutionContext = executionContext("ingestion-worker")

  def store(measurements: List[MetricMeasurement]): Unit = {
    Future {
      log.info(s"Received samples of ${measurements.length} metrics")
      try {
        metricMeasurementStore.save(measurements)
      } catch {
        case reason: Throwable ⇒ log.error("Error storing measurements", reason)
      }
    }
  }
}
