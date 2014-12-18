package com.despegar.khronus.influx.store

import com.despegar.khronus.influx.Influx
import com.despegar.khronus.influx.finder.InfluxDashboardResolver
import com.despegar.khronus.store.CassandraKeyspace

object CassandraDashboards extends CassandraKeyspace {
  initialize
  val influxDashboardResolver = new InfluxDashboardResolver(session)

  override def keyspace = "dashboards"

  override def getRF: Int = Influx().Settings.rf

}
