package com.despegar.khronus.store

import com.despegar.khronus.model.{ Metric, MetricMeasurement, _ }
import com.despegar.khronus.util.{ Measurable, Settings, ConcurrencySupport }
import com.despegar.khronus.util.log.Logging
import org.HdrHistogram.Histogram

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

trait MetricMeasurementStoreSupport {
  def metricStore: MetricMeasurementStore = CassandraMetricMeasurementStore
}

trait MetricMeasurementStore {
  def storeMetricMeasurements(metricMeasurements: List[MetricMeasurement])
}

object CassandraMetricMeasurementStore extends MetricMeasurementStore with BucketSupport with MetaSupport with Logging with ConcurrencySupport with MonitoringSupport with TimeWindowsSupport with Measurable {

  implicit val executionContext: ExecutionContext = executionContext("metric-receiver-worker")

  private val rawDuration = 1 millis
  private val storeGroupDuration = 5 seconds

  def storeMetricMeasurements(metricMeasurements: List[MetricMeasurement]) = {
    try {
      store(metricMeasurements)
    } catch {
      case reason: Throwable ⇒ log.error("Failed receiving samples", reason)
    }
  }

  private def store(metrics: List[MetricMeasurement]) = measureTime("measurementStore.store", "store metricMeasurements") {
    log.info(s"Received samples of ${metrics.length} metrics")
    metrics foreach storeMetric
  }

  private def storeMetric(metricMeasurement: MetricMeasurement): Unit = measureTime("measurementStore.storeMetric", "store metric") {
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

  private def track(metric: Metric) = measureTime("measurementStore.track", "track metric") {
    metaStore.getFromSnapshot.get(metric) map { case (timestamp, active) => metaStore.notifyMetricMeasurement(metric, active) } getOrElse {
      log.debug(s"Got a new metric: $metric. Will store metadata for it")
      storeMetadata(metric)
    }
  }

  private def storeMetadata(metric: Metric) = measureFutureTime("measurementStore.storeMetadata", "store metadata") {
    metaStore.insert(metric)
  }

  private def storeHistogramMetric(metric: Metric, metricMeasurement: MetricMeasurement) = {
    val histogram = HistogramBucket.newHistogram
    storeGrouped(metric, metricMeasurement) { (bucketNumber, measurements) ⇒
      histogram.reset()
      measurements.foreach(measurement ⇒ skipNegativeValues(metricMeasurement, measurement.values).foreach(value ⇒ histogram.recordValue(value)))
      histogramBucketStore.store(metric, rawDuration, Seq(new HistogramBucket(bucketNumber, histogram)))
    }
  }

  private def storeGrouped[T](metric: Metric, metricMeasurement: MetricMeasurement)(block: (BucketNumber, List[Measurement]) ⇒ Future[Unit]): Unit = {
    val groupedMeasurements = metricMeasurement.measurements.groupBy(measurement ⇒ Timestamp(measurement.ts).alignedTo(storeGroupDuration))
    groupedMeasurements.foldLeft(Future.successful(())) { (previousStore, measurementsGroup) ⇒
      previousStore.flatMap { _ ⇒
        val timestamp = measurementsGroup._1
        val bucketNumber = timestamp.toBucketNumberOf(rawDuration)
        if (!alreadyProcessed(metric, bucketNumber)) {
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
      counterBucketStore.store(metric, rawDuration, Seq(new CounterBucket(bucketNumber, counts)))
    }
  }

  private def skipNegativeValues(metricMeasurement: MetricMeasurement, values: Seq[Long]): Seq[Long] = {
    val (invalidValues, okValues) = values.partition(value ⇒ value < 0)
    if (!invalidValues.isEmpty)
      log.warn(s"Skipping invalid values for metric $metricMeasurement: $invalidValues")
    okValues
  }

  private def alreadyProcessed(metric: Metric, rawBucketNumber: BucketNumber) = {
    //get the bucket number in the smallest window duration
    val measureBucket = rawBucketNumber ~ smallestWindow.duration
    //get the current tick. The delay is to softly avoid out of sync clocks between nodes (another node start to process the tick)
    if (Tick.alreadyProcessed(rawBucketNumber)) {
      log.warn(s"Measurements for $metric marked to be reprocessed because their bucket number ($measureBucket) is less or equals than the current bucket tick (${Tick().bucketNumber})")
    }
    false
  }

}