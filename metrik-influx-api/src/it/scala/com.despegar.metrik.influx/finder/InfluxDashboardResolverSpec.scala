/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.despegar.metrik.influx.finder

import org.scalatest.{ FunSuite, Matchers }
import scala.concurrent.Await
import scala.concurrent.duration._
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.connectionpool.OperationResult
import com.despegar.metrik.store.CassandraCounterBucketStore
import com.despegar.metrik.influx.service.Dashboard
import org.apache.commons.codec.binary.Base64

class InfluxDashboardResolverSpec extends FunSuite with Matchers with BaseInfluxIntegrationSpec {

  override def columnFamilies = Seq(InfluxDashboardResolver.Column)

  test("Store dashboard saves dashboard ok") {
    val plainName = "dashboardName"
    val dashboard = getDashboard(plainName)
    val futureStore = InfluxDashboardResolver.store(dashboard)
    val result = await(futureStore)

    result should be (plainName)

    val dashboards = await(InfluxDashboardResolver.lookup(plainName))
    dashboards.size should be(1)
    dashboards(0).name should be(dashboard.name)
    dashboards(0).columns should be(dashboard.columns)
    dashboards(0).points should be(dashboard.points)
  }


  test("list dashboards returns all dashboards that matches criteria") {
    val dashboardTest1 = getDashboard("test1")
    await(InfluxDashboardResolver.store(dashboardTest1))

    val otroDashboard = getDashboard("otroDashboard")
    await(InfluxDashboardResolver.store(otroDashboard))

    val dashboardTest2 = getDashboard("test2")
    await(InfluxDashboardResolver.store(dashboardTest2))

    val criteria = "test"
    val listDashboardsGrafanaExpression = s"select * from /grafana.dashboard_.*/ where  title =~ /.*$criteria.*/i&time_precision=s"
    val futureDashboards = InfluxDashboardResolver.dashboardOperation(listDashboardsGrafanaExpression)

    val results = await(futureDashboards)

    results.size should be(2)
    results(0).name should be(dashboardTest1.name)
    results(1).name should be(dashboardTest2.name)
  }

  test("Get dashboard returns the dashboard ok") {
    val dashboardTest = getDashboard("test")
    await(InfluxDashboardResolver.store(dashboardTest))

    val encodedName = dashboardTest.name
    val getDashboardGrafanaExpression = s"""select dashboard from \"grafana.dashboard_$encodedName\"&time_precision=s"""
    val futureDashboard = InfluxDashboardResolver.dashboardOperation(getDashboardGrafanaExpression)

    val result = await(futureDashboard)
    result.size should be(1)
    result(0).name should be(dashboardTest.name)
  }


  test("Drop dashboard deletes dashboard ok") {
    val plainName = "dashboardName"
    val dashboard = getDashboard(plainName)
    await(InfluxDashboardResolver.store(dashboard))

    await(InfluxDashboardResolver.lookup(plainName)).size should be(1)

    // Drop
    val encodedName = dashboard.name
    val dropDashboardGrafanaExpression = s"""drop series \"grafana.dashboard_$encodedName\""""
    await(InfluxDashboardResolver.dashboardOperation(dropDashboardGrafanaExpression))

    await(InfluxDashboardResolver.lookup(plainName)).size should be(0)
  }

  test("Unknown grafana expression throws exception") {
    val unknownGrafanaExpression = "Unknown grafana expression"
    intercept[UnsupportedOperationException] {InfluxDashboardResolver.dashboardOperation(unknownGrafanaExpression)}
  }

  private def getDashboard(dashboardName: String): Dashboard = {
    val timestamp = System.currentTimeMillis().toString
    val columns = Vector("time", "sequence_number", "title", "tags", "dashboard", "id")
    val points = Vector(Vector(timestamp, "123", "Title", "", "{}", "dashboard1"))

    Dashboard(Base64.encodeBase64String(dashboardName.getBytes()), columns, points)
  }

}