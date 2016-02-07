package com.searchlight.khronus.query

import com.searchlight.khronus.model.{ Metric, SubMetric, Timestamp }
import com.searchlight.khronus.store.MetaStore
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

class QueryPlannerTest extends FunSuite with Matchers with MockitoSugar {

  val cpuHistogram: Metric = Metric("cpu", "histogram")
  val cpuNode1 = SubMetric(cpuHistogram, Map("node" -> "node1"))
  val cpuNode2 = SubMetric(cpuHistogram, Map("node" -> "node2"))
  val someTimeRange: TimeRange = TimeRange(Timestamp(1), Timestamp(2))
  val metrics = Map(cpuHistogram -> Seq(cpuNode1, cpuNode2))
  val metaMock = mock[MetaStore]
  Mockito.when(metaMock.getMetricsMap).thenReturn(metrics)
  val queryPlanner = new SimpleQueryPlanner {
    override val metaStore = metaMock
  }

  test("select one metric query") {
    val cpu: QMetric = QMetric("cpu", "c")

    val query = DynamicQuery(Seq(Count("c")), Seq(cpu), Equals("c", "node", "node1"), someTimeRange)

    val result = queryPlanner.getQueryPlan(query)

    result.subMetrics should have size 1
    result.subMetrics(cpu) should have size 1
    result.subMetrics(cpu) should equal(Seq(cpuNode1))
  }

  test("select two metric query") {
    val c = QMetric("cpu", "c")
    val d = QMetric("cpu", "d")

    val query = DynamicQuery(Seq(Count("c"), Count("d")), Seq(c, d), And(Equals("c", "node", "node1"), Equals("d", "node", "node2")), someTimeRange)

    val result = queryPlanner.getQueryPlan(query)

    result.subMetrics should have size 2
    result.subMetrics(c) should have size 1
    result.subMetrics(c) should equal(Seq(cpuNode1))
    result.subMetrics(d) should have size 1
    result.subMetrics(d) should equal(Seq(cpuNode2))
  }

}
