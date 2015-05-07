package com.despegar.khronus.store

import com.despegar.khronus.model.{ Metric, MetricMeasurement, _ }
import com.despegar.khronus.util.ConcurrencySupport
import com.despegar.khronus.util.log.Logging

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

trait MetricMeasurementStoreSupport {
  def metricStore: MetricMeasurementStore = CassandraMetricMeasurementStore
}

trait MetricMeasurementStore {
  def storeMetricMeasurements(metricMeasurements: List[MetricMeasurement])
}

object CassandraMetricMeasurementStore extends MetricMeasurementStore with BucketSupport with MetaSupport with Logging with ConcurrencySupport {

  implicit val executionContext: ExecutionContext = executionContext("metric-receiver-worker")

  def storeMetricMeasurements(metricMeasurements: List[MetricMeasurement]) = {
    store(metricMeasurements)
  }

  private def store(metrics: List[MetricMeasurement]) = {
    log.info(s"Received ${metrics.length} metrics to be stored")
    metrics foreach storeMetric
  }

  private def storeMetric(metricMeasurement: MetricMeasurement): Unit = {
    if (metricMeasurement.measurements.isEmpty) {
      log.warn(s"Discarding store of ${metricMeasurement.asMetric} with empty measurements")
      return
    }
    val metric = metricMeasurement.asMetric
    log.trace(s"Storing metric $metric")
    metric.mtype match {
      case MetricType.Timer | MetricType.Gauge ⇒ storeHistogramMetric(metric, metricMeasurement)
      case MetricType.Counter                  ⇒ storeCounterMetric(metric, metricMeasurement)
      case _ ⇒ {
        val msg = s"Discarding $metric. Unknown metric type: ${metric.mtype}"
        log.warn(msg)
        throw new UnsupportedOperationException(msg)
      }
    }
    track(metric)
  }

  private def track(metric: Metric) = {
    if (isNew(metric)) {
      log.debug(s"Got a new metric: $metric. Will store metadata for it")
      storeMetadata(metric)
    } else {
      log.trace(s"$metric is already known. No need to store meta for it")
    }
  }

  private def storeMetadata(metric: Metric) = metaStore.insert(metric)

  private def storeHistogramMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    storeGrouped(metric, metricMeasurement) { (bucketNumber, measurements) ⇒
      val histogram = HistogramBucket.newHistogram
      measurements.foreach(measurement ⇒ skipNegativeValues(metricMeasurement, measurement.values).foreach(value ⇒ histogram.recordValue(value)))
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
    } onFailure { case e: Exception ⇒ log.error(s"Fail to store submitted metric for $metric with measures ${metricMeasurement.measurements}", e) }
  }

  private def storeCounterMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    storeGrouped(metric, metricMeasurement) { (bucketNumber, measurements) ⇒
      val counts = measurements.map(measurement ⇒ skipNegativeValues(metricMeasurement, measurement.values).sum).sum
      counterBucketStore.store(metric, 1 millis, Seq(new CounterBucket(bucketNumber, counts)))
    }
  }

  private def skipNegativeValues(metricMeasurement: MetricMeasurement, values: Seq[Long]): Seq[Long] = {
    val (invalidValues, okValues) = values.partition(value ⇒ value < 0)
    if (!invalidValues.isEmpty)
      log.warn(s"Skipping invalid values for metric $metricMeasurement: $invalidValues")
    okValues
  }

  private def alreadyProcessed(bucketNumber: BucketNumber) = false //how?

  //ok, this has to be improved. maybe scheduling a reload at some interval and only going to meta if not found
  private def isNew(metric: Metric) = !metaStore.contains(metric)

}