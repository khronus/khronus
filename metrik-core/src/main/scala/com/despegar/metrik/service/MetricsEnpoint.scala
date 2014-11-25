package com.despegar.metrik.service

import akka.actor.Props
import com.despegar.metrik.model.MetricBatchProtocol._
import com.despegar.metrik.model.{ Metric, MetricBatch, MetricMeasurement, _ }
import com.despegar.metrik.store.{ BucketSupport, MetaSupport }
import com.despegar.metrik.util.log.Logging
import spray.http.StatusCodes._
import spray.routing.{ HttpService, HttpServiceActor, Route }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class MetrikActor extends HttpServiceActor with MetricsEnpoint with MetrikHandlerException {
  def receive = runRoute(metricsRoute)
}

object MetrikActor {
  val Name = "metrik-actor"
  val Path = "metrik/metrics"

  def props = Props[MetrikActor]
}

trait MetricsEnpoint extends HttpService with BucketSupport with MetaSupport with Logging {

  override def loggerName = classOf[MetricsEnpoint].getName()

  val metricsRoute: Route =
    post {
      entity(as[MetricBatch]) { metricBatch ⇒
          complete {
            store(metricBatch.metrics)
            OK
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

    log.debug(s"Storing metric $metric")

    metric.mtype match {
      case MetricType.Timer   ⇒ storeHistogramMetric(metric, metricMeasurement)
      case MetricType.Counter ⇒ storeCounterMetric(metric, metricMeasurement)
      case anythingElse ⇒
        val msg = s"Discarding $metric. Unknown metric type: ${metric.mtype}"
        log.warn(msg)
        throw new UnsupportedOperationException(msg)
    }
    //only if it's of a known type
    track(metric)
  }

  private def track(metric: Metric): Unit = {
    if (isNew(metric)) {
      log.info(s"Got a new metric: $metric. Will store metadata for it")
      storeMetadata(metric)
    } else {
      log.info(s"$metric is already known. No need to store meta for it")
    }
  }

  private def storeMetadata(metric: Metric) = metaStore.insert(metric)

  private def storeHistogramMetric(metric: Metric, metricMeasurement: MetricMeasurement): Unit = {
    storeGrouped(metric, metricMeasurement) { (bucketNumber, measurements) ⇒
      val histogram = HistogramBucket.newHistogram
      measurements.foreach(measurement ⇒ measurement.values.foreach(value ⇒ histogram.recordValue(value)))
      histogramBucketStore.store(metric, 1 millis, Seq(new HistogramBucket(bucketNumber, histogram)))
    }
  }

  private def storeGrouped(metric: Metric, metricMeasurement: MetricMeasurement)(block: (BucketNumber, List[Measurement]) ⇒ Future[Unit]): Unit = {
    val groupedMeasurements = metricMeasurement.measurements.groupBy(measurement ⇒ Timestamp(measurement.ts).alignedTo(5 seconds))
    groupedMeasurements.foldLeft(Future.successful(())) { (acc, measurementsGroup) ⇒
      acc.flatMap { _ ⇒
        val timestamp = measurementsGroup._1
        val bucketNumber = timestamp.toBucketNumber(1 millis)
        if (!alreadyProcessed(bucketNumber)) {
          block(bucketNumber, measurementsGroup._2)
        } else {
          Future.successful(())
        }
      }
    }
  }

  private def storeCounterMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    storeGrouped(metric, metricMeasurement) { (bucketNumber, measurements) ⇒
      counterBucketStore.store(metric, 1 millis, Seq(new CounterBucket(bucketNumber, measurements.map(_.values.sum).sum)))
    }
  }

  private def alreadyProcessed(bucketNumber: BucketNumber) = false //how?

  //ok, this has to be improved. maybe scheduling a reload at some interval and only going to meta if not found
  private def isNew(metric: Metric) = !metaStore.contains(metric)
}