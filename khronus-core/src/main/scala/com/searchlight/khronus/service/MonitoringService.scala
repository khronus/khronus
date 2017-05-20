package com.searchlight.khronus.service

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ConcurrentLinkedQueue, TimeUnit }

import com.searchlight.khronus.model.{ Measurement, MetricMeasurement }
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, Settings }

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Buffer

trait MonitoringSupport {
  def recordHistogram(name: String, value: Long): Unit = MonitoringService.recordHistogram(name, value)

  def recordGauge(name: String, value: Long): Unit = MonitoringService.recordGauge(name, value)

  def incrementCounter(name: String): Unit = incrementCounter(name, 1)

  def incrementCounter(name: String, counts: Int): Unit = incrementCounter(name, counts.toLong)

  def incrementCounter(name: String, counts: Long): Unit = MonitoringService.incrementCounter(name, counts)
}

object MonitoringService extends Logging with ConcurrencySupport {

  private val histograms = TrieMap[String, ConcurrentLinkedQueue[java.lang.Long]]()
  private val gauges = TrieMap[String, ConcurrentLinkedQueue[java.lang.Long]]()
  private val counters = TrieMap[String, AtomicLong]()

  private val enabled = Settings.InternalMetrics.Enabled
  private val ingestionService = IngestionService()

  enabled {
    val scheduler = scheduledThreadPool("monitoring-flusher-worker")
    scheduler.scheduleAtFixedRate(new Runnable() {
      override def run() = flush()
    }, 0, 10, TimeUnit.SECONDS)
  }

  def recordHistogram(name: String, value: Long) = enabled {
    val values = histograms.getOrElseUpdate(name, new ConcurrentLinkedQueue[java.lang.Long]())
    values.offer(value)
  }

  def recordGauge(name: String, value: Long) = enabled {
    val values = gauges.getOrElseUpdate(name, new ConcurrentLinkedQueue[java.lang.Long]())
    values.offer(value)
  }

  def incrementCounter(name: String, counts: Long) = enabled {
    val counter = counters.getOrElseUpdate(name, new AtomicLong())
    counter.addAndGet(counts)
  }

  private def enabled(block: ⇒ Unit): Unit = {
    if (enabled) block
  }

  private def flush() = {
    try {
      write(measurements())
    } catch {
      case e: Throwable ⇒ log.error(s"Error flushing monitoring metrics: ${e.getMessage}", e)
    }
  }

  private def write(metricMeasurements: List[MetricMeasurement]) = {
    if (metricMeasurements.nonEmpty) {
      ingestionService.store(metricMeasurements)
    }
  }

  private def drain(queue: ConcurrentLinkedQueue[java.lang.Long]): Seq[Long] = {
    var value = queue.poll()
    val values = Buffer[Long]()
    while (value != null) {
      values += value
      value = queue.poll()
    }
    values
  }

  private def measurements(): List[MetricMeasurement] = {
    val measurements = counters.flatMap { case (name, counts) ⇒ collectCounterValues(name, counts) } ++
      histograms.flatMap { case (name, values) ⇒ collectHistogramValues(name, values) } ++
      gauges.flatMap { case (name, values) ⇒ collectGaugeValues(name, values) }
    measurements.toList
  }

  private def collectCounterValues(name: String, counts: AtomicLong): Option[MetricMeasurement] = {
    Some(MetricMeasurement(system(name), "counter", List(Measurement(Some(System.currentTimeMillis()), Seq(counts.getAndSet(0))))))
  }

  private def collectHistogramValues(name: String, values: ConcurrentLinkedQueue[java.lang.Long]): Option[MetricMeasurement] = {
    val histogramValues: Seq[Long] = drain(values)
    if (histogramValues.nonEmpty)
      Some(MetricMeasurement(system(name), "histogram", List(Measurement(Some(System.currentTimeMillis()), histogramValues))))
    else
      None
  }

  private def collectGaugeValues(name: String, values: ConcurrentLinkedQueue[java.lang.Long]): Option[MetricMeasurement] = {
    val gaugeValues = drain(values)
    if (gaugeValues.nonEmpty) {
      Some(MetricMeasurement(system(name), "gauge", List(Measurement(Some(System.currentTimeMillis()), gaugeValues))))
    } else
      None
  }

  private def system(metricName: String) = s"~system.$metricName"

}