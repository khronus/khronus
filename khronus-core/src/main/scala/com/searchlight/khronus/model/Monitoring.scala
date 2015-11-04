package com.searchlight.khronus.model

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ConcurrentLinkedQueue, TimeUnit }

import com.searchlight.khronus.store.MetricMeasurementStoreSupport
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, Settings }

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Buffer

trait MonitoringSupport {
  def recordTime(metricName: String, time: Long): Unit = Monitoring.recordTime(metricName, time)

  def recordGauge(metricName: String, value: Long): Unit = Monitoring.recordGauge(metricName, value)

  def incrementCounter(metricName: String): Unit = incrementCounter(metricName, 1)

  def incrementCounter(metricName: String, counts: Int): Unit = incrementCounter(metricName, counts.toLong)

  def incrementCounter(metricName: String, counts: Long): Unit = Monitoring.incrementCounter(metricName, counts)
}

object Monitoring extends MetricMeasurementStoreSupport with Logging with ConcurrencySupport {

  private val timers = TrieMap[String, ConcurrentLinkedQueue[java.lang.Long]]()
  private val gauges = TrieMap[String, ConcurrentLinkedQueue[java.lang.Long]]()
  private val counters = TrieMap[String, AtomicLong]()

  private val enabled = Settings.InternalMetrics.Enabled

  enabled {
    val scheduler = scheduledThreadPool("monitoring-flusher-worker")
    scheduler.scheduleAtFixedRate(new Runnable() {
      override def run() = flush()
    }, 0, 10, TimeUnit.SECONDS)
  }

  def recordTime(metricName: String, value: Long) = enabled {
    val values = timers.getOrElseUpdate(metricName, new ConcurrentLinkedQueue[java.lang.Long]())
    values.offer(value)
  }

  def recordGauge(metricName: String, value: Long) = enabled {
    val values = gauges.getOrElseUpdate(metricName, new ConcurrentLinkedQueue[java.lang.Long]())
    values.offer(value)
  }

  def incrementCounter(metricName: String, counts: Long) = enabled {
    val counter = counters.getOrElseUpdate(metricName, new AtomicLong())
    counter.addAndGet(counts)
  }

  private def enabled(block: ⇒ Unit): Unit = {
    if (enabled) block
  }

  private def flush() = {
    try {
      write(measurements())
    } catch {
      case e: Throwable ⇒ log.error(s"Error flushing monitoring metrics: ${e.getMessage()}", e)
    }
  }

  private def write(metricMeasurements: List[MetricMeasurement]) = {
    if (!metricMeasurements.isEmpty) {
      metricStore.storeMetricMeasurements(metricMeasurements)
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
    val measurements = counters.map { case (metricName, counts) ⇒ collectCounterValues(metricName, counts) } ++
      timers.map { case (metricName, values) ⇒ collectHistogramValues(metricName, values, "timer") } ++
      gauges.map { case (metricName, values) ⇒ collectHistogramValues(metricName, values, "gauge") }
    measurements.toList
  }

  private def collectCounterValues(metricName: String, counts: AtomicLong): MetricMeasurement = {
    MetricMeasurement(system(metricName), "counter", List(Measurement(Some(System.currentTimeMillis()), Seq(counts.getAndSet(0)))))
  }

  private def collectHistogramValues(metricName: String, values: ConcurrentLinkedQueue[java.lang.Long], mtype: String): MetricMeasurement = {
    MetricMeasurement(system(metricName), mtype, List(Measurement(Some(System.currentTimeMillis()), drain(values))))
  }

  private def system(metricName: String) = s"~system.$metricName"

}