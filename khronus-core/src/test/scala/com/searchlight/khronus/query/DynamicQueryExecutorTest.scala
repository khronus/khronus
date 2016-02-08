package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import org.HdrHistogram.Histogram
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class DynamicQueryExecutorTest extends FunSuite with Matchers with MockitoSugar {

  val someEvent_qmetric: QMetric = QMetric("some_event", "e")
  val someEvent_metric: Metric = Metric("some_event", "counter")
  val someEvent_submetric1 = SubMetric(someEvent_metric, Map("tag1" -> "someValue", "tag2" -> "otherValue1"))
  val someEvent_submetric2 = SubMetric(someEvent_metric, Map("tag1" -> "someValue", "tag2" -> "otherValue2"))

  val someTimer_qmetric: QMetric = QMetric("some_timer", "t")
  val someTimer_metric: Metric = Metric("some_timer", "histogram")
  val someTimer_submetric1 = SubMetric(someTimer_metric, Map("tag1" -> "someValue", "tag2" -> "otherValue1"))
  val someTimer_submetric2 = SubMetric(someTimer_metric, Map("tag1" -> "someValue", "tag2" -> "otherValue2"))

  test("select count(e) from some_event e where e.tag1 = 'someValue' ") {
    val query: DynamicQuery = DynamicQuery(Seq(Count("e")),
      Seq(someEvent_qmetric), Equals("e", "tag1", "someValue"), TimeRange(Timestamp(1), Timestamp(2)))

    val metricSelectorMock: QueryPlanner = mock[QueryPlanner]
    when(metricSelectorMock.getQueryPlan(query)).thenReturn(
      QueryPlan(Map(someEvent_qmetric -> Seq(someEvent_submetric1, someEvent_submetric2))))

    val bucketQueryExecutorMock: BucketQueryExecutor = mock[BucketQueryExecutor]
    val counterBucket: CounterBucket = new CounterBucket(BucketNumber(1L, 1 minute), 1L)
    when(bucketQueryExecutorMock.retrieve(someEvent_submetric1, query.range)).thenReturn(Future.successful(BucketSlice(counterBucket)))
    when(bucketQueryExecutorMock.retrieve(someEvent_submetric2, query.range)).thenReturn(Future.successful(BucketSlice(counterBucket)))

    val dynamicQueryExecutor = new DefaultDynamicQueryExecutor {
      override val queryPlanner = metricSelectorMock
      override val bucketQueryExecutor = bucketQueryExecutorMock
    }

    val future = dynamicQueryExecutor.execute(query)

    val series = Await.result(future, 3 seconds)

    series should have size 1
    series.head.name should equal("some_event.count")
    series.head.points should equal(Seq(Point(Timestamp(60000), 2d)))
  }

  test("select percentiles(t, 20, 50, 95, 99.9) from some_timer t where e.tag1 = 'someValue' ") {
    val query: DynamicQuery = DynamicQuery(Seq(Percentiles("t", Seq(20, 50, 95, 99.9))),
      Seq(someTimer_qmetric), Equals("t", "tag1", "someValue"), TimeRange(Timestamp(1), Timestamp(2)))

    val metricSelectorMock: QueryPlanner = mock[QueryPlanner]
    when(metricSelectorMock.getQueryPlan(query)).thenReturn(
      QueryPlan(Map(someTimer_qmetric -> Seq(someTimer_submetric1, someTimer_submetric2))))

    val bucketQueryExecutorMock: BucketQueryExecutor = mock[BucketQueryExecutor]
    val histo1 = new Histogram(1000, 3)
    (1 to 500) foreach (n ⇒ histo1.recordValue(n))
    val histo2 = new Histogram(1000, 3)
    (501 to 1000) foreach (n ⇒ histo2.recordValue(n))
    val bucket1: HistogramBucket = new HistogramBucket(BucketNumber(1L, 1 minute), histo1)
    val bucket2: HistogramBucket = new HistogramBucket(BucketNumber(1L, 1 minute), histo2)

    when(bucketQueryExecutorMock.retrieve(someTimer_submetric1, query.range)).thenReturn(Future.successful(BucketSlice(bucket1)))
    when(bucketQueryExecutorMock.retrieve(someTimer_submetric2, query.range)).thenReturn(Future.successful(BucketSlice(bucket2)))

    val dynamicQueryExecutor = new DefaultDynamicQueryExecutor {
      override val queryPlanner = metricSelectorMock
      override val bucketQueryExecutor = bucketQueryExecutorMock
    }

    val future = dynamicQueryExecutor.execute(query)

    val series = Await.result(future, 3 seconds)

    series should have size 4
    series should contain(Series("some_timer.p20.0", Seq(Point(Timestamp(60000), 200))))
    series should contain(Series("some_timer.p50.0", Seq(Point(Timestamp(60000), 500))))
    series should contain(Series("some_timer.p95.0", Seq(Point(Timestamp(60000), 950))))
    series should contain(Series("some_timer.p99.9", Seq(Point(Timestamp(60000), 999))))
  }

}
