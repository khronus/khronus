package com.searchlight.khronus.dao

import com.searchlight.khronus.model._
import com.searchlight.khronus.service.{ CassandraBucketService, BucketService, MonitoringSupport }
import com.searchlight.khronus.util.Measurable
import com.searchlight.khronus.util.log.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait MetricMeasurementStore {

  trait Support {
    def metricMeasurementStore: MetricMeasurementStore = MetricMeasurementStore

  }

  def save(metricMeasurements: List[MetricMeasurement])
}

object MetricMeasurementStore extends MetricMeasurementStore with MetaSupport with Logging with MonitoringSupport with TimeWindowsSupport with Measurable {

  private val rawDuration = 1 millis
  private val storeGroupDuration = 5 seconds
  private val bucketService: BucketService = CassandraBucketService()

  def save(measurements: List[MetricMeasurement]) = measureTime("measurementStore.store", "store metricMeasurements") {
    measureFutureTime("measurementStore.store.futures", "store metricMeasurements futures") {
      Future.sequence(collectBuckets(measurements) map {
        case (metric, buckets) ⇒
          bucketService.save(metric, buckets)
      })
    }
  }

  private def collectBuckets(metrics: List[MetricMeasurement]): Map[Metric, Seq[Bucket]] = {
    val now = System.currentTimeMillis()
    metrics.groupBy(measurement ⇒ measurement.asMetric).map {
      case (metric, measurements) ⇒
        track(metric)
        val buckets = measurements.flatMap { metricMeasurement ⇒
          val groupedMeasurements = metricMeasurement.measurements.groupBy(measurement ⇒ Timestamp(measurement.ts.getOrElse(now)).alignedTo(storeGroupDuration))
          buildBuckets(metric, groupedMeasurements)
        }
        (metric, buckets)
    }
  }

  private def buildBuckets(metric: Metric, groupedMeasurements: Map[Timestamp, List[Measurement]]): Seq[Bucket] = {
    groupedMeasurements.toSeq.view.map {
      case (timestamp, measures) ⇒
        metric.mtype.bucketWithMeasures(metric, timestamp.toBucketNumberOf(rawDuration), measures)
    }
  }

  private def track(metric: Metric) = measureTime("measurementStore.track", "track metric") {
    metaStore.snapshot.get(metric) collect { case (timestamp, active) ⇒ metaStore.notifyMetricMeasurement(metric, active) } getOrElse {
      log.debug(s"Got a new metric: $metric. Will store metadata for it")
      storeMetadata(metric)
    }
  }

  private def storeMetadata(metric: Metric) = measureFutureTime("measurementStore.storeMetadata", "store metadata") {
    metaStore.insert(metric)
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