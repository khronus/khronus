package com.searchlight.khronus.query

import com.searchlight.khronus.dao.MetaStore
import com.searchlight.khronus.model._
import com.searchlight.khronus.model.query._
import com.searchlight.khronus.service.QueryPlanService
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

class QueryPlanServiceTest extends FunSuite with Matchers with MockitoSugar {

  val cpuNode1 = Metric("cpu", "histogram", Map("node" -> "node1"))
  val cpuNode2 = Metric("cpu", "histogram", Map("node" -> "node2"))
  val someTimeRange = Some(TimeRange(Timestamp(1).ms, Timestamp(2).ms))
  val metrics = Map("cpu" -> Seq(cpuNode1, cpuNode2))
  val metaMock = mock[MetaStore]
  Mockito.when(metaMock.getMetricsMap).thenReturn(metrics)

  val queryPlanner = QueryPlanService(metaMock)

  test("select one metric query") {
    val cpu = Selector("cpu", Some("c"))

    val query = Query(Seq(Count(cpu)), Seq(cpu), someTimeRange, Some(Equals(cpu, "node", "node1")))

    val result = queryPlanner.calculateQueryPlan(query)

    result.selectedMetrics should have size 1
    result.selectedMetrics(cpu) should have size 1
    result.selectedMetrics(cpu) should equal(Seq(cpuNode1))
  }

  test("select two metric query") {
    val c = Selector("cpu", Some("c"))
    val d = Selector("cpu", Some("d"))

    val query = Query(Seq(Count(c), Count(d)), Seq(c, d), someTimeRange, Some(And(Seq(Equals(c, "node", "node1"), Equals(d, "node", "node2")))))

    val result = queryPlanner.calculateQueryPlan(query)

    result.selectedMetrics should have size 2
    result.selectedMetrics(c) should have size 1
    result.selectedMetrics(c) should equal(Seq(cpuNode1))
    result.selectedMetrics(d) should have size 1
    result.selectedMetrics(d) should equal(Seq(cpuNode2))
  }

}
