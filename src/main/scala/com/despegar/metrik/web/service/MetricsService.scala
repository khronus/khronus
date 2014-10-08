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

trait MetricsService extends HttpService with HistogramBucketSupport with MetricSupport {

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

  def store(metrics: List[Metric]) = metrics foreach storeMetric

  def storeMetric(metric: Metric) = {
    track(metric)
    metric.mtype match {
      case "timer" => storeHistogramMetric(metric)
      case "gauge" => storeHistogramMetric(metric)
      case "counter" => throw new UnsupportedOperationException("Counters not yet supported")
    }
  }

  def track(metric: Metric) = if (isNew(metric)) storeMetadata(metric)

  def storeMetadata(metric: Metric) = metricStore.store(metric)

  def storeHistogramMetric(metric: Metric) = histogramBucketStore.store(metric.name, 1 millis, metric.asHistogramBuckets.filter(!alreadyProcessed(_)))

  def alreadyProcessed(histogramBucket: HistogramBucket) = false

  def isNew(metric: Metric) = true

}