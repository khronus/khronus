package com.despegar.khronus.influx.store

import com.despegar.khronus.influx.Influx
import com.despegar.khronus.influx.finder.InfluxDashboardResolver
import com.despegar.khronus.store.CassandraSupport

object CassandraDashboards extends CassandraDashboards {
  initialize
}

trait CassandraDashboards extends CassandraSupport {
  override def keyspace = "dashboards"

  override def getRF: Int = Influx().Settings.rf

  override def initialize: Unit = {
    super.initialize
    retry(MaxRetries, "Creating dashboard table") { InfluxDashboardResolver.initialize }
  }
}
