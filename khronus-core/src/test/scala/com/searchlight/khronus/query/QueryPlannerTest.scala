package com.searchlight.khronus.query

import com.searchlight.khronus.model.{Metric, Timestamp}
import com.searchlight.khronus.query.projection.Count
import com.searchlight.khronus.store.MetaStore
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

class QueryPlannerTest extends FunSuite with Matchers with MockitoSugar {

  val cpuNode1 = Metric("cpu", "histogram", Map("node" -> "node1"))
  val cpuNode2 = Metric("cpu", "histogram", Map("node" -> "node2"))
  val someTimeRange = Slice(Timestamp(1).ms, Timestamp(2).ms)
  val metrics = Map("cpu" -> Seq(cpuNode1, cpuNode2))
  val metaMock = mock[MetaStore]
  Mockito.when(metaMock.getMetricsMap).thenReturn(metrics)

  val queryPlanner = new DefaultQueryPlanner {
    override val metaStore = metaMock
  }

  test("select one metric query") {
    val cpu: QMetric = QMetric("cpu", "c")

    val query = DynamicQuery(Seq(Count("c")), Seq(cpu), Some(Equals("c", "node", "node1")), someTimeRange)

    val result = queryPlanner.calculateQueryPlan(query)

    result.metrics should have size 1
    result.metrics(cpu) should have size 1
    result.metrics(cpu) should equal(Seq(cpuNode1))
  }

  test("select two metric query") {
    val c = QMetric("cpu", "c")
    val d = QMetric("cpu", "d")

    val query = DynamicQuery(Seq(Count("c"), Count("d")), Seq(c, d), Some(And(Equals("c", "node", "node1"), Equals("d", "node", "node2"))), someTimeRange)

    val result = queryPlanner.calculateQueryPlan(query)

    result.metrics should have size 2
    result.metrics(c) should have size 1
    result.metrics(c) should equal(Seq(cpuNode1))
    result.metrics(d) should have size 1
    result.metrics(d) should equal(Seq(cpuNode2))
  }

}
