package com.despegar.khronus.model

import java.util.concurrent.{ ConcurrentLinkedQueue, Executors, TimeUnit }

import com.despegar.khronus.store.MetricMeasurementStoreSupport
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ ConcurrencySupport, Settings }

import scala.collection.mutable.{ Buffer, Map }

trait MonitoringSupport {
  def recordTime(metricName: String, time: Long): Unit = Monitoring.recordTime(metricName, time)

  def recordGauge(metricName: String, value: Long): Unit = Monitoring.recordGauge(metricName, value)

  def incrementCounter(metricName: String): Unit = incrementCounter(metricName, 1)

  def incrementCounter(metricName: String, counts: Int): Unit = incrementCounter(metricName, counts.toLong)

  def incrementCounter(metricName: String, counts: Long): Unit = Monitoring.incrementCounter(metricName, counts)

}

object Monitoring extends MetricMeasurementStoreSupport with Logging with ConcurrencySupport {

  val queue = new ConcurrentLinkedQueue[MonitoringMetric]()

  val enabled = Settings.InternalMetrics.Enabled

  val schedule = enabled {
    val scheduler = scheduledThreadPool("monitoring-flusher-worker")
    scheduler.scheduleAtFixedRate(new Runnable() {
      override def run() = flush
    }, 0, 2, TimeUnit.SECONDS)

  }

  def enabled(block: ⇒ Unit): Unit = {
    if (enabled) block
  }

  def flush() = {
    try {
      write(drained(queue))
    } catch {
      case e: Throwable ⇒ log.error(s"Error flushing monitoring metrics: ${e.getMessage()}", e)
    }
  }

  private def write(metrics: Seq[MonitoringMetric]) = {
    val rawMetricMeasurements = Map[String, Map[String, Map[Long, Buffer[Long]]]]()
    metrics.foreach { metric ⇒
      val mtypeMap = rawMetricMeasurements.getOrElseUpdate(metric.mtype, Map[String, Map[Long, Buffer[Long]]]())
      val metricMap = mtypeMap.getOrElseUpdate(metric.name, Map[Long, Buffer[Long]]())
      val values = metricMap.getOrElseUpdate(metric.timestamp, Buffer[Long]())
      values += metric.value
    }
    val metricMeasurements = rawMetricMeasurements.collect {
      case (mtype, mtypeMap) ⇒
        mtypeMap.collect {
          case (metricName, rawMeasurements) ⇒
            MetricMeasurement(s"~system.$metricName", mtype, rawMeasurements.collect { case (ts, value) ⇒ Measurement(ts, value.toSeq) }.toList)
        }
    }.toList.flatten

    metricStore.storeMetricMeasurements(metricMeasurements)
  }

  private def drained(queue: ConcurrentLinkedQueue[MonitoringMetric]): Seq[MonitoringMetric] = {
    var metric = queue.poll()
    val metrics = Buffer[MonitoringMetric]()
    while (metric != null) {
      metrics += metric
      metric = queue.poll()
    }
    metrics
  }

  def recordTime(metricName: String, value: Long) = enabled {
    queue.offer(TimerValue(metricName, value, System.currentTimeMillis()))
  }

  def recordGauge(metricName: String, value: Long) = enabled {
    queue.offer(GaugeValue(metricName, value, System.currentTimeMillis()))
  }

  def incrementCounter(metricName: String, counts: Long) = enabled {
    queue.offer(Counter(metricName, counts, System.currentTimeMillis()))
  }

  case class Counter(name: String, value: Long, timestamp: Long) extends MonitoringMetric {
    val mtype = "counter"
  }

  case class TimerValue(name: String, value: Long, timestamp: Long) extends MonitoringMetric {
    val mtype = "timer"
  }

  case class GaugeValue(name: String, value: Long, timestamp: Long) extends MonitoringMetric {
    val mtype = "gauge"
  }

  trait MonitoringMetric {
    def value: Long

    def name: String

    def mtype: String

    def timestamp: Long
  }

}

