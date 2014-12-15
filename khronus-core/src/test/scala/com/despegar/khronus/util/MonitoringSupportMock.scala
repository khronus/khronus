package com.despegar.khronus.util

import com.despegar.khronus.model.MonitoringSupport

trait MonitoringSupportMock extends MonitoringSupport {
  override def recordTime(metricName: String, time: Long): Unit = {}

  override def recordGauge(metricName: String, value: Long): Unit = {}

  override def incrementCounter(metricName: String): Unit = {}

  override def incrementCounter(metricName: String, counts: Int): Unit = {}

  override def incrementCounter(metricName: String, counts: Long): Unit = {}
}
