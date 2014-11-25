package com.despegar.metrik.model

import java.util.concurrent.{TimeUnit, Executors, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicReference
import com.despegar.metrik.store.MetricMeasurementStoreSupport

import scala.collection.mutable.{Map, Buffer}
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._

trait MonitoringSupport {

  def recordValue(metricName: String, value: Long) = Monitoring.recordValue(metricName, value)

  def incrementCounter(metricName: String) = Monitoring.incrementCounter(metricName)

}

object Monitoring extends MetricMeasurementStoreSupport {

 // val queue = new AtomicReference[ConcurrentLinkedQueue[MonitoringMetric]](new ConcurrentLinkedQueue[MonitoringMetric]())
  val queue = new ConcurrentLinkedQueue[MonitoringMetric]()

  val scheduler = Executors.newScheduledThreadPool(1)
  implicit val executor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  scheduler.scheduleAtFixedRate(new Runnable() {
    override def run() = flush
  }, 0, 5, TimeUnit.SECONDS)

  def flush() = write(drained(queue))

  private def write(metrics: Seq[MonitoringMetric]) = {
    val map = Map[String,Map[Long,Buffer[Long]]]()
    metrics.foreach{ metric =>
        val metricMap = map.getOrElseUpdate(metric.name,Map[Long,Buffer[Long]]())

    }
  }

  private def drained(queue: ConcurrentLinkedQueue[MonitoringMetric]): Seq[MonitoringMetric] = {
    var metric = queue.poll()
    val metrics = Buffer[MonitoringMetric]()
    while(metric != null) {
      metrics += metric
      metric = queue.poll()
    }
    metrics
  }

  def recordValue(metricName: String, value: Long) = {
    queue.offer(HistogramValue(metricName, value, System.currentTimeMillis()))
  }

  def incrementCounter(metricName: String) = {
    queue.offer(Counter(metricName, 1, System.currentTimeMillis()))
  }

  case class Counter(name: String, value: Long, timestamp: Long) extends MonitoringMetric {

  }

  case class HistogramValue(name: String, value: Long, timestamp: Long) extends MonitoringMetric

  trait MonitoringMetric {
    def value: Long
    def name: String
    def timestamp: Long
  }
}

