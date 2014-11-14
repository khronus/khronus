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

import java.util.concurrent.Executors

import com.despegar.metrik.influx.service.Dashboard
import com.despegar.metrik.store.Cassandra
import com.despegar.metrik.util.{ KryoSerializer, Logging }
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.{ ColumnFamily, ColumnList }
import com.netflix.astyanax.serializers.StringSerializer
import org.apache.commons.codec.binary.Base64

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NonFatal

trait DashboardResolver {
  def dashboardOperation(expression: String): Future[Seq[Dashboard]]

  def store(dashboard: Dashboard): Future[String]
}

trait DashboardSupport {
  def dashboardResolver: DashboardResolver = InfluxDashboardResolver
}

object InfluxDashboardResolver extends DashboardResolver with Logging {

  //extract Z3JhZmFuYTIy from (select dashboard from "grafana.dashboard_Z3JhZmFuYTIy"&time_precision=s)
  private val GetDashboardPattern = "select.*_(.*)\".*".r

  //extract .*grafana.* from (select * from /grafana.dashboard_.*/ where  title =~ /.*grafana.*/i&time_precision=s)
  private val ListDashboardsPattern = "select.*/(.*)/..*".r

  // extract Z3JhZmFuYTI= from (drop+series+"grafana.dashboard_Z3JhZmFuYTI=")
  private val DropDashboardPattern = "drop.*_(.*)\".*".r

  private val Dispatcher = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  val Row = "dashboards"
  val Column = ColumnFamily.newColumnFamily("dashboards", StringSerializer.get(), StringSerializer.get())
  val Serializer: KryoSerializer[Dashboard] = new KryoSerializer(Row, List(Dashboard.getClass))

  def initialize = Cassandra.createColumnFamily(Column)

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
  }

  def lookup(expression: String): Future[Seq[Dashboard]] = executeWithinFuture {
    log.debug(s"Looking for Dashboard with expression: $expression}")

    val columns: OperationResult[ColumnList[String]] = Cassandra.keyspace.prepareQuery(Column).getKey(Row).execute()
    columns.getResult.asScala.filter(_.getName.matches(expression))
      .map(column ⇒ Serializer.deserialize(column.getByteArrayValue))(collection.breakOut)
  }

  def drop(dashboard: String): Future[Seq[Dashboard]] = executeWithinFuture {
    log.info(s"Deleting dashboard: $dashboard")

    Cassandra.keyspace.prepareColumnMutation(Column, Row, dashboard).deleteColumn().execute()
    Seq.empty
  }

  def store(dashboard: Dashboard): Future[String] = {
    val name = new String(Base64.decodeBase64(dashboard.name.split("_").last))
    log.debug(s"Storing dashboard with name: ${name}")

    executeWithinFuture {
      val mutation = Cassandra.keyspace.prepareMutationBatch()
      mutation.withRow(Column, Row).putColumn(name, Serializer.serialize(dashboard))
      mutation.execute
      name
    }
  }

  private def executeWithinFuture[T](thunk: ⇒ T): Future[T] = {
    val p = Promise[T]()
    try {
      p.completeWith(Future(thunk)(Dispatcher))
    } catch {
      case NonFatal(reason) ⇒
        log.error("Error trying to execute operation", reason)
        p.failure(reason)
    }
    p.future
  }
}

