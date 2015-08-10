package com.searchlight.khronus.influx.store

import com.searchlight.khronus.influx.Influx
import com.searchlight.khronus.influx.finder.InfluxDashboardResolver
import com.searchlight.khronus.store.CassandraKeyspace

object CassandraDashboards extends CassandraKeyspace {
  initialize
  val influxDashboardResolver = new InfluxDashboardResolver(session)

  override def keyspace = "dashboards"

  override def getRF: Int = Influx().Settings.rf

}
