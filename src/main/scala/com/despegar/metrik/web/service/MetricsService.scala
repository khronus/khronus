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
import com.despegar.metrik.store.HistogramBucketSupport
import spray.http.StatusCodes._
import com.despegar.metrik.model.MetricBatch
import com.despegar.metrik.model.MetricMeasurement
import com.despegar.metrik.util.Logging
import com.despegar.metrik.store.MetaSupport
import scala.concurrent.ExecutionContext.Implicits.global
import com.despegar.metrik.model.Metric

trait MetricsService extends HttpService with HistogramBucketSupport with MetaSupport with Logging {

  override def loggerName = classOf[MetricsService].getName()

  val metricsRoute =
    path("metrik" / "metrics") {
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
    }

  private def store(metrics: List[MetricMeasurement]) = {
    log.info(s"Received ${metrics.length} metrics to be stored")
    metrics foreach storeMetric
  }

  private def storeMetric(metricMeasurement: MetricMeasurement) = {
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
    bucketStore.store(metric, 1 millis, metricMeasurement.asHistogramBuckets.filter(!alreadyProcessed(_)))
  }

  private def storeCounterMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    bucketStore.store(metric, 1 millis, metricMeasurement.asHistogramBuckets.filter(!alreadyProcessed(_)))
  }

  private def alreadyProcessed(histogramBucket: HistogramBucket) = false //how?

  //ok, this has to be improved. maybe scheduling a reload at some interval and only going to meta if not found
  private def isNew(metric: Metric) = metaStore.retrieveMetrics map { !_.contains(metric) }

}