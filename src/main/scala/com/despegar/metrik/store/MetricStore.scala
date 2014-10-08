package com.despegar.metrik.store

import com.despegar.metrik.model.Metric

trait MetricStore {
  def store(metric: Metric)
}

trait MetricSupport {
  def metricStore:MetricStore = CassandraMetricStore
}

object CassandraMetricStore extends MetricStore {
  
  def store(metric: Metric) = {
    
  }
  
}