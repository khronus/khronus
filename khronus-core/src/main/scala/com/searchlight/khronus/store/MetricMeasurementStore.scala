package com.searchlight.khronus.store

import com.searchlight.khronus.model.{ Metric, MetricMeasurement, _ }
import com.searchlight.khronus.util.{ Measurable, Settings, ConcurrencySupport }
import com.searchlight.khronus.util.log.Logging
import org.HdrHistogram.Histogram

import scala.collection.mutable
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
    val histos = mutable.Buffer[(Metric, () ⇒ HistogramBucket)]()
    val counters = mutable.Buffer[(Metric, () ⇒ CounterBucket)]()

    val now = System.currentTimeMillis()

    metrics foreach (metricMeasurement ⇒ {
      val metric = metricMeasurement.asMetric
      val groupedMeasurements = metricMeasurement.measurements.groupBy(measurement ⇒ Timestamp(measurement.ts.getOrElse(now)).alignedTo(storeGroupDuration))

      metric.mtype match {
        case MetricType.Timer | MetricType.Gauge ⇒ histos ++= buildHistogramBuckets(metric, groupedMeasurements)
        case MetricType.Counter                  ⇒ counters ++= buildCounterBuckets(metric, groupedMeasurements)
        case _ ⇒ {
          val msg = s"Discarding samples of $metric. Unknown metric type: ${metric.mtype}"
          log.warn(msg)
        }
      }
    })

    val histogramsFuture = histogramBucketStore.store(histos, rawDuration)
    val countersFuture = counterBucketStore.store(counters, rawDuration)

    measureFutureTime("measurementStore.store.futures", "store metricMeasurements futures")(Future.sequence(Seq(histogramsFuture, countersFuture)))
  }

  private def buildHistogramBuckets(metric: Metric, groupedMeasurements: Map[Timestamp, List[Measurement]]): List[(Metric, () ⇒ HistogramBucket)] = {
    track(metric)
    groupedMeasurements.toList.map {
      case (timestamp, measures) ⇒
        (metric, () ⇒ {
          val histogram = HistogramBucket.newHistogram(maxValue(measures))
          val bucketNumber = timestamp.toBucketNumberOf(rawDuration)
          measures.foreach(measure ⇒ record(metric, measure, histogram))
          new HistogramBucket(bucketNumber, histogram)
        })
    }
  }

  private def maxValue(measurements: List[Measurement]) = {
    var maxValue = 0L
    measurements.foreach { measurement ⇒
      if (measurement.values.nonEmpty) {
        val value = measurement.values.max
        if (value > maxValue) {
          maxValue = value
        }
      }
    }
    maxValue
  }

  def record(metric: Metric, measure: Measurement, histogram: Histogram): Unit = {
    skipNegativeValues(metric, measure.values).foreach(value ⇒ {
      val highestTrackableValue = histogram.getHighestTrackableValue
      if (value <= highestTrackableValue) histogram.recordValue(value)
      else {
        val exceeded = value - highestTrackableValue
        log.warn(s"Sample of $metric has exceeded the highestTrackableValue of $highestTrackableValue by $exceeded. Truncating the excedent. Try changing the sampling unit or increasing the highestTrackableValue")
        histogram.recordValue(highestTrackableValue)
      }
    })
  }

  private def buildCounterBuckets(metric: Metric, groupedMeasurements: Map[Timestamp, List[Measurement]]): List[(Metric, () ⇒ CounterBucket)] = {
    track(metric)
    groupedMeasurements.toList.map {
      case (timestamp, measures) ⇒
        (metric, () ⇒ {
          val bucketNumber = timestamp.toBucketNumberOf(rawDuration)
          val counts = measures.map(measure ⇒ skipNegativeValues(metric, measure.values).sum).sum
          new CounterBucket(bucketNumber, counts)
        })
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

  private def skipNegativeValues(metric: Metric, values: Seq[Long]): Seq[Long] = {
    val (invalidValues, okValues) = values.partition(value ⇒ value < 0)
    if (invalidValues.nonEmpty)
      log.warn(s"Skipping invalid values for metric $metric: $invalidValues")
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