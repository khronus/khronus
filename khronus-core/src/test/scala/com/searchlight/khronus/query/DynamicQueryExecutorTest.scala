package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class DynamicQueryExecutorTest extends FunSuite with Matchers with MockitoSugar {

  test("select count(e) from some_event e where e.tag1 = 'someValue' ") {
    val someEvent: QMetric = QMetric("some_event", "e")
    val query: DynamicQuery = DynamicQuery(Seq(Count("e")),
      Seq(someEvent), Equals("e", "tag1", "someValue"), TimeRange(Timestamp(1), Timestamp(2)))

    val metricSelectorMock: QueryPlanner = mock[QueryPlanner]

    val metric: Metric = Metric("some_event", "counter")
    val subMetric1 = SubMetric(metric, Map("tag1" -> "someValue", "tag2" -> "otherValue1"))
    val subMetric2 = SubMetric(metric, Map("tag1" -> "someValue", "tag2" -> "otherValue2"))

    when(metricSelectorMock.getQueryPlan(query)).thenReturn(
      QueryPlan(Map(someEvent -> Seq(subMetric1, subMetric2))))

    val executor: BucketQueryExecutor = mock[BucketQueryExecutor]
    val counterBucket: CounterBucket = new CounterBucket(BucketNumber(1L, 1 minute), 1L)
    when(executor.retrieve(subMetric1, query.range)).thenReturn(Future.successful(BucketSlice(counterBucket)))
    when(executor.retrieve(subMetric2, query.range)).thenReturn(Future.successful(BucketSlice(counterBucket)))

    trait BucketQueryExecutorMockSupport extends BucketQuerySupport {
      override val bucketQueryExecutor = executor
    }

    val dynamicQueryExecutor = new DefaultDynamicQueryExecutor with BucketQueryExecutorMockSupport {
      override val queryPlanner = metricSelectorMock
    }
    val future = dynamicQueryExecutor.execute(query)

    val series = Await.result(future, 3 seconds)

    series should have size 1
    series.head.name should equal("some_event")
    series.head.points should equal(Seq(Point(Timestamp(60000), 2d)))

  }

}
