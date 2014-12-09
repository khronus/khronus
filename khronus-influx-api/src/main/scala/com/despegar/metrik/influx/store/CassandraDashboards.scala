package com.despegar.metrik.influx.store

import com.despegar.metrik.influx.Influx
import com.despegar.metrik.influx.finder.InfluxDashboardResolver
import com.despegar.metrik.store.CassandraSupport

object CassandraDashboards extends CassandraSupport {
  override def keyspace = "dashboards"

  override def getRF: Int = Influx().Settings.rf

  override def initialize: Unit = {
    super.initialize
    retry(MaxRetries, "Creating dashboard table") { InfluxDashboardResolver.initialize }
  }
}
