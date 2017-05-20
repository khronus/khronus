package com.searchlight.khronus.influx.store

import com.searchlight.khronus.influx.Influx
import com.searchlight.khronus.influx.finder.InfluxDashboardResolver
import com.searchlight.khronus.dao.CassandraKeyspace

object CassandraDashboards extends CassandraKeyspace {
  val influxDashboardResolver = new InfluxDashboardResolver(session)

  override def keyspace = "dashboards"

  override def getRF: Int = Influx().Settings.rf

}
