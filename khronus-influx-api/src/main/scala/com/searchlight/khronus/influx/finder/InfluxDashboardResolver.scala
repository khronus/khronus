/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.searchlight.khronus.influx.finder

import java.nio.ByteBuffer

import com.datastax.driver.core.utils.Bytes
import com.datastax.driver.core.{ ResultSet, Session }
import com.searchlight.khronus.influx.service.Dashboard
import com.searchlight.khronus.influx.store.CassandraDashboards
import com.searchlight.khronus.store.CassandraUtils
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, KryoSerializer }
import org.apache.commons.codec.binary.Base64

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure

trait DashboardResolver {
  def dashboardOperation(expression: String): Future[Seq[Dashboard]]

  def store(dashboard: Dashboard): Future[String]
}

trait DashboardSupport {
  def dashboardResolver: DashboardResolver = CassandraDashboards.influxDashboardResolver
}

class InfluxDashboardResolver(session: Session) extends DashboardResolver with Logging with CassandraUtils with ConcurrencySupport {

  //extract Z3JhZmFuYTIy from (select dashboard from "grafana.dashboard_Z3JhZmFuYTIy"&time_precision=s)
  private val GetDashboardPattern = "select.*_(.*)\".*".r

  //extract .*grafana.* from (select * from /grafana.dashboard_.*/ where  title =~ /.*grafana.*/i&time_precision=s)
  private val ListDashboardsPattern = "select.*/(.*)/..*".r

  // extract Z3JhZmFuYTI= from (drop+series+"grafana.dashboard_Z3JhZmFuYTI=")
  private val DropDashboardPattern = "drop.*_(.*)\".*".r

  implicit val Dispatcher = executionContext("influx-dashboard-resolver-worker", 4)

  val DashboardsKey = "dashboards"

  retry(MaxRetries, "Creating dashboard table") {
    session.execute(s"create table if not exists dashboard (key text, name text, content blob, primary key (key, name));")
  }

  private val GetAllStmt = session.prepare("select name, content from dashboard where key = ?;")
  private val DeleteStmt = session.prepare("delete from dashboard where key = ? and name = ?;")
  private val InsertStmt = session.prepare("insert into dashboard (key, name, content) values (?, ?, ?);")

  val Serializer: KryoSerializer[Dashboard] = new KryoSerializer(DashboardsKey, List(Dashboard.getClass))

  def dashboardOperation(expression: String): Future[Seq[Dashboard]] = expression match {
    case GetDashboardPattern(group) ⇒ {
      val dashboardName = new String(Base64.decodeBase64(group.toString))
      lookup(dashboardName)
    }
    case ListDashboardsPattern(group) ⇒ {
      val dashboardsExpression = s"(?i)$group"
      lookup(dashboardsExpression)
    }
    case DropDashboardPattern(group) ⇒ {
      val dashboardName = new String(Base64.decodeBase64(group.toString))
      drop(dashboardName)
    }
    case _ ⇒ {
      log.error(s"Unsupported grafana expression [$expression]")
      throw new UnsupportedOperationException(s"Unsupported grafana expression [$expression]")
    }
  }

  def lookup(expression: String): Future[Seq[Dashboard]] = {
    log.debug(s"Looking for Dashboard with expression: $expression}")

    val future: Future[ResultSet] = session.executeAsync(GetAllStmt.bind(DashboardsKey))
    future.
      map(resultSet ⇒ {
        val dashboards = resultSet.all().asScala.filter(_.getString("name").matches(expression)).map(row ⇒ Serializer.deserialize(Bytes.getArray(row.getBytes("content"))))
        log.debug(s"Found ${dashboards.length} dashboards")
        dashboards
      }).
      andThen { case Failure(reason) ⇒ log.error(s"Failed to retrieve dashboards", reason) }
  }

  def drop(dashboardName: String): Future[Seq[Dashboard]] = {
    log.info(s"Deleting dashboard: $dashboardName")

    val future: Future[ResultSet] = session.executeAsync(DeleteStmt.bind(DashboardsKey, dashboardName))
    future.
      map(_ ⇒ Seq.empty[Dashboard]).
      andThen { case Failure(reason) ⇒ log.error(s"Failed to delete dashboard $dashboardName", reason) }
  }

  def store(dashboard: Dashboard): Future[String] = {
    val name = new String(Base64.decodeBase64(dashboard.name.split("_").last))
    log.debug(s"Storing dashboard with name: ${name}")

    val future: Future[ResultSet] = session.executeAsync(InsertStmt.bind(DashboardsKey, name, ByteBuffer.wrap(Serializer.serialize(dashboard))))
    future.
      map(_ ⇒ name).
      andThen { case Failure(reason) ⇒ log.error(s"Failed to save dashboard ${dashboard.name}", reason) }
  }

}

