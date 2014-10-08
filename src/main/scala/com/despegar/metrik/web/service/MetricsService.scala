package com.despegar.metrik.web.service

import spray.routing.HttpService
import spray.routing._
import spray.http.MediaTypes._
import spray.httpx.unmarshalling._
import spray.json.DefaultJsonProtocol
import com.despegar.metrik.model.MetricBatchProtocol._
import org.HdrHistogram.Histogram
import com.despegar.metrik.store.CassandraHistogramBucketStore
import com.despegar.metrik.model.HistogramBucket
import scala.concurrent.duration._
import com.despegar.metrik.store.MetricSupport
import com.despegar.metrik.store.HistogramBucketSupport
import spray.http.StatusCodes._
import com.despegar.metrik.model.MetricBatch
import com.despegar.metrik.model.Metric
import com.despegar.metrik.util.Logging

trait MetricsService extends HttpService with HistogramBucketSupport with MetricSupport with Logging {

  override def loggerName = classOf[MetricsService].getName()
  
  val metricsRoute = 
    path("metrik" / "metrics") {
      post {
        entity(as[MetricBatch]) { metricBatch =>
          respondWithStatus(OK) {
            complete {
              store(metricBatch.metrics)
              metricBatch
            }
          }
        }
      }
    }

  private def store(metrics: List[Metric]) = {
    log.info(s"Received ${metrics.length} metrics to be stored")
    metrics foreach storeMetric
  }

  private def storeMetric(metric: Metric) = {
    track(metric)
    log.debug(s"Storing metric $metric")
    metric.mtype match {
      case "timer" => storeHistogramMetric(metric)
      case "gauge" => storeHistogramMetric(metric)
      case "counter" => throw new UnsupportedOperationException("Counters not yet supported")
    }
  }

  private def track(metric: Metric) = if (isNew(metric)) {
    log.info(s"Got a new metric: $metric. Will store metadata for it")
    storeMetadata(metric)
  }

  private def storeMetadata(metric: Metric) = metricStore.store(metric)

  private def storeHistogramMetric(metric: Metric) = {
    histogramBucketStore.store(metric.name, 1 millis, metric.asHistogramBuckets.filter(!alreadyProcessed(_)))
  }

  private def alreadyProcessed(histogramBucket: HistogramBucket) = false

  private def isNew(metric: Metric) = true

}