package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import com.searchlight.khronus.model.bucket.{ CounterBucket, HistogramBucket }
import com.searchlight.khronus.model.query._
import com.searchlight.khronus.service._
import org.HdrHistogram.Histogram
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class BucketQueryServiceTest extends FunSuite with Matchers with MockitoSugar {

  val someEvent_selector = Selector("some_event", Some("e"))
  val someEvent_metric1 = Metric("some_event", "counter", Map("tag1" -> "someValue", "tag2" -> "otherValue1"))
  val someEvent_metric2 = Metric("some_event", "counter", Map("tag1" -> "someValue", "tag2" -> "otherValue2"))

  val someTimer_expression = Selector("some_timer", Some("t"))
  val someTimer_metric1 = Metric("some_timer", "histogram", Map("tag1" -> "someValue", "tag2" -> "otherValue1"))
  val someTimer_metric2 = Metric("some_timer", "histogram", Map("tag1" -> "someValue", "tag2" -> "otherValue2"))

  test("select count(e) from some_event e where e.tag1 = 'someValue' ") {
    val query: Query = Query(Seq(Count(someEvent_selector)),
      Seq(someEvent_selector), Some(TimeRange(Timestamp(1).ms, Timestamp(2).ms)), Some(Equals(someEvent_selector, "tag1", "someValue")), Some(1 minute))

    val queryPlannerMock = mock[QueryPlanService]
    when(queryPlannerMock.calculateQueryPlan(query)).thenReturn(
      QueryPlan(Map(someEvent_selector -> Seq(someEvent_metric1, someEvent_metric2))))

    val bucketServiceMock = mock[BucketService]
    val counterBucket = CounterBucket(BucketNumber(1L, 1 minute), 1L)
    when(bucketServiceMock.retrieve(someEvent_metric1, query.timeRange.get, query.resolution.get)).thenReturn(Future.successful(BucketSlice(counterBucket)))
    when(bucketServiceMock.retrieve(someEvent_metric2, query.timeRange.get, query.resolution.get)).thenReturn(Future.successful(BucketSlice(counterBucket)))

    val bucketQueryService = BucketQueryService(queryPlanner = queryPlannerMock, bucketService = bucketServiceMock)

    val future = bucketQueryService.execute(query)

    val series = Await.result(future, 10 seconds)

    series should have size 1
    series.head.name should equal("e_count")
    series.head.points should equal(Seq(Point(Timestamp(60000).ms, 2d)))
  }

  test("select count(t), percentiles(t, 20, 50, 95, 99.9) from some_timer t where e.tag1 = 'someValue' ") {
    val query: Query = Query(Seq(Count(someTimer_expression), Percentile(someTimer_expression, 20), Percentile(someTimer_expression, 50), Percentile(someTimer_expression, 95), Percentile(someTimer_expression, 99.9)),
      Seq(someTimer_expression), Some(TimeRange(Timestamp(1).ms, Timestamp(2).ms)), Some(Equals(someTimer_expression, "tag1", "someValue")), Some(1 minute))

    val queryPlannerMock = mock[QueryPlanService]
    when(queryPlannerMock.calculateQueryPlan(query)).thenReturn(
      QueryPlan(Map(someTimer_expression -> Seq(someTimer_metric1, someTimer_metric2))))

    val bucketServiceMock = mock[BucketService]
    val histo1 = new Histogram(1000, 3)
    (1 to 500) foreach (n ⇒ histo1.recordValue(n))

    val histo2 = new Histogram(1000, 3)
    (501 to 1000) foreach (n ⇒ histo2.recordValue(n))

    val bucket1 = HistogramBucket(BucketNumber(1L, 1 minute), histo1)
    val bucket2 = HistogramBucket(BucketNumber(1L, 1 minute), histo2)

    when(bucketServiceMock.retrieve(someTimer_metric1, query.timeRange.get, query.resolution.get)).thenReturn(Future.successful(BucketSlice(bucket1)))
    when(bucketServiceMock.retrieve(someTimer_metric2, query.timeRange.get, query.resolution.get)).thenReturn(Future.successful(BucketSlice(bucket2)))

    val bucketQueryService = BucketQueryService(queryPlanner = queryPlannerMock, bucketService = bucketServiceMock)

    val future = bucketQueryService.execute(query)

    val series = Await.result(future, 3 seconds)

    series should have size 5
    series should contain(Series("t_count", Seq(Point(Timestamp(60000).ms, 1000))))
    series should contain(Series("t_20.0%", Seq(Point(Timestamp(60000).ms, 200))))
    series should contain(Series("t_50.0%", Seq(Point(Timestamp(60000).ms, 500))))
    series should contain(Series("t_95.0%", Seq(Point(Timestamp(60000).ms, 950))))
    series should contain(Series("t_99.9%", Seq(Point(Timestamp(60000).ms, 999))))
  }

}
