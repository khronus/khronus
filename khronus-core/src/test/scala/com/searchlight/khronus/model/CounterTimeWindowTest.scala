package com.searchlight.khronus.model

import java.io.IOException

import com.searchlight.khronus.model.BucketNumber._
import com.searchlight.khronus.model.Timestamp._
import com.searchlight.khronus.store._
import com.searchlight.khronus.util.MonitoringSupportMock
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NoStackTrace

class CounterTimeWindowTest extends FunSuite with MockitoSugar with TimeWindowTest[CounterBucket] {

  val metric = Metric("metrickA", MetricType.Counter)

  test("with previous buckets should store its buckets and summaries and remove previous buckets") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = BucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets).map(tuple ⇒ BucketResult(tuple._1, tuple._2)))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = CounterSummary(0, 2)
    val summaryBucketB = CounterSummary(30000, 1)
    val mySummaries = Seq(summaryBucketA, summaryBucketB)

    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBucketsMap))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, tick), 5 seconds)

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify summary statistics store
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets removal
    //verify(window.bucketStore).remove(metric, previousWindowDuration, uniqueTimestampsPreviousBuckets)
  }

  test("without previous buckets should do nothing") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(BucketSlice[CounterBucket](Seq())))
    when(window.bucketStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))

    //call method to test
    val tick = Tick(BucketNumber(15000, windowDuration))
    Await.result(window.process(metric, tick), 5 seconds)

    //verify that not store any temporal bucket
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify that not remove anything
    //verify(window.bucketStore).remove(metric, previousWindowDuration, Seq())
  }

  test("should do nothing upon failure of previous buckets slice retrieval") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], Matchers.any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future.failed(new IOException()))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))

    //call method to test
    val tick = Tick(BucketNumber(15000, windowDuration))
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify that not store any temporal bucket
    verify(window.bucketStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[CounterBucket]])

    //verify that not store any summary
    verify(window.summaryStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[CounterSummary]])

    //verify that not remove anything
    //verify(window.bucketStore, never()).remove(any[Metric], any[FiniteDuration], any[Seq[Timestamp]])
  }

  test("with previous buckets should not remove them upon failure of temporal buckets store") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = BucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets).map(tuple ⇒ BucketResult(tuple._1, tuple._2)))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBucketsMap))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future.failed(new IOException("Expected exception in Test") with NoStackTrace))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify previous buckets were not removed
    //verify(window.bucketStore, never()).remove(metric, previousWindowDuration, uniqueTimestampsPreviousBuckets)
  }

  test("with previous buckets should not remove them upon failure of summaries store") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = BucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets).map(tuple ⇒ BucketResult(tuple._1, tuple._2)))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = CounterSummary(0, 2)
    val summaryBucketB = CounterSummary(30000, 1)
    val mySummaries = Seq(summaryBucketA, summaryBucketB)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBucketsMap))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future.failed(new IOException()))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets were not removed
    //verify(window.bucketStore, never()).remove(metric, previousWindowDuration, uniqueTimestampsPreviousBuckets)
  }

  private def mockedWindow(windowDuration: FiniteDuration, previousWindowDuration: FiniteDuration) = {
    val window = new CounterTimeWindow(windowDuration, previousWindowDuration) with CounterBucketStoreSupport with CounterSummaryStoreSupport with MetaSupport with MonitoringSupportMock {
      override val bucketStore = mock[BucketStore[CounterBucket]]

      override val summaryStore = mock[SummaryStore[CounterSummary]]

      override val metaStore = mock[MetaStore]
    }
    window
  }

}