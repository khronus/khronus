package com.despegar.metrik.web.service

import akka.actor.Props
import spray.routing._
import spray.httpx.unmarshalling._
import com.despegar.metrik.model.MetricBatchProtocol._
import scala.concurrent.duration._
import spray.http.StatusCodes._
import com.despegar.metrik.model.MetricBatch
import com.despegar.metrik.model.MetricMeasurement
import com.despegar.metrik.util.Logging
import com.despegar.metrik.store.MetaSupport
import scala.concurrent.ExecutionContext.Implicits.global
import com.despegar.metrik.model.Metric
import com.despegar.metrik.store.BucketSupport
import com.despegar.metrik.model.Bucket

class MetrikActor extends HttpServiceActor with MetricsEnpoint {
  def receive = runRoute(metricsRoute)
}

object MetrikActor {
  def props = Props[MetrikActor]
}

trait MetricsEnpoint extends HttpService with BucketSupport with MetaSupport with Logging {

  override def loggerName = classOf[MetricsEnpoint].getName()

  val metricsRoute: Route =
    post {
      entity(as[MetricBatch]) { metricBatch ⇒
        respondWithStatus(OK) {
          complete {
            store(metricBatch.metrics)
            metricBatch
          }
        }
      }
    }

  private def store(metrics: List[MetricMeasurement]) = {
    log.info(s"Received ${metrics.length} metrics to be stored")
    metrics foreach storeMetric
  }

  private def storeMetric(metricMeasurement: MetricMeasurement): Unit = {
    if (metricMeasurement.measurements.isEmpty) {
      log.warn(s"Discarding post of ${metricMeasurement.asMetric} with empty measurements")
      return
    }
    val metric = metricMeasurement.asMetric
    track(metric)
    log.debug(s"Storing metric $metric")
    metric.mtype match {
      case ("timer" | "gauge") ⇒ storeHistogramMetric(metric, metricMeasurement)
      case "counter"           ⇒ storeCounterMetric(metric, metricMeasurement)
      case _ ⇒ {
        val msg = s"Discarding $metric. Unknown metric type: ${metric.mtype}"
        log.warn(msg)
        throw new UnsupportedOperationException(msg)
      }
    }
  }

  private def track(metric: Metric) = {
    isNew(metric) map { isNew ⇒
      if (isNew) {
        log.info(s"Got a new metric: $metric. Will store metadata for it")
        storeMetadata(metric)
      } else {
        log.info(s"$metric is already known. No need to store meta for it")
      }
    }
  }

  private def storeMetadata(metric: Metric) = metaStore.insert(metric)

  private def storeHistogramMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    histogramBucketStore.store(metric, 1 millis, metricMeasurement.asHistogramBuckets.filter(!alreadyProcessed(_)))
  }

  private def storeCounterMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    counterBucketStore.store(metric, 1 millis, metricMeasurement.asCounterBuckets.filter(!alreadyProcessed(_)))
  }

  private def alreadyProcessed[T <: Bucket](bucket: T) = false //how?

  //ok, this has to be improved. maybe scheduling a reload at some interval and only going to meta if not found
  private def isNew(metric: Metric) = metaStore.retrieveMetrics map { !_.contains(metric) }

}