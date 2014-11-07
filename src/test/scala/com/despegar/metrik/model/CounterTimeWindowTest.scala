package com.despegar.metrik.model

import com.despegar.metrik.store._
import org.HdrHistogram.Histogram
import org.mockito.{ Matchers, ArgumentMatcher, ArgumentCaptor, Mockito }
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{ FunSuite }
import org.scalatest.mock.MockitoSugar
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.io.IOException
import com.despegar.metrik.model.Timestamp._
import com.despegar.metrik.model.BucketNumber._

class CounterTimeWindowTest extends FunSuite with MockitoSugar {

  val metric = Metric("metrickA", "counter")
  val windowDuration: FiniteDuration = 30 seconds
  val previousWindowDuration: FiniteDuration = 1 millis
  val neverProcessedTimestamp = Timestamp(-1)

  test("with previous buckets should store its buckets and summaries and remove previous buckets") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = CounterSummary(0, 2)
    val summaryBucketB = CounterSummary(30000, 1)
    val mySummaries = Seq(summaryBucketA, summaryBucketB)

    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, previousBuckets)).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, tick), 5 seconds)

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify summary statistics store
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets removal
    verify(window.bucketStore).remove(metric, previousWindowDuration, previousBuckets)
  }

  test("with already processed buckets should remove them without storing any bucket or summary") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data that for any reason was not deleted! (already processed)
    val somePreviousBucket = new CounterBucket((15000, previousWindowDuration), 1)
    val previousUndeletedBuckets = Seq(somePreviousBucket)
    val tick = Tick(somePreviousBucket.bucketNumber ~ windowDuration)

    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousUndeletedBuckets))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](60000L))
    when(window.bucketStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, previousUndeletedBuckets)).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, tick), 5 seconds)

    //verify that not store any temporal histogram
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify removal of previous undeleted buckets
    verify(window.bucketStore).remove(metric, previousWindowDuration, previousUndeletedBuckets)
  }

  test("without previous buckets should do nothing") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(Seq()))
    when(window.bucketStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, Seq())).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))

    //call method to test
    Await.result(window.process(metric, Tick.current(Seq(window))), 5 seconds)

    //verify that not store any temporal bucket
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify that not remove anything
    verify(window.bucketStore).remove(metric, previousWindowDuration, Seq())
  }

  test("should do nothing upon failure of previous buckets slice retrieval") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future.failed(new IOException()))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, Tick.current(Seq(window))), 5 seconds)
    }

    //verify that not store any temporal bucket
    verify(window.bucketStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[CounterBucket]])

    //verify that not store any summary
    verify(window.summaryStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[CounterSummary]])

    //verify that not remove anything
    verify(window.bucketStore, never()).remove(any[Metric], any[FiniteDuration], any[Seq[CounterBucket]])
  }

  test("with previous buckets should not remove them upon failure of temporal buckets store") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future.failed(new IOException()))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify previous buckets were not removed
    verify(window.bucketStore, never()).remove(metric, previousWindowDuration, previousBuckets)
  }

  test("with previous buckets should not remove them upon failure of summaries store") {

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new CounterBucket((1, previousWindowDuration), 1)
    val previousBucket2 = new CounterBucket((2, previousWindowDuration), 1)
    val previousBucket3 = new CounterBucket((30001, previousWindowDuration), 1)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val bucketA = new CounterBucket((0, windowDuration), 2)
    val bucketB = new CounterBucket((1, windowDuration), 1)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = CounterSummary(0, 2)
    val summaryBucketB = CounterSummary(30000, 1)
    val mySummaries = Seq(summaryBucketA, summaryBucketB)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future.failed(new IOException()))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets were not removed
    verify(window.bucketStore, never()).remove(metric, previousWindowDuration, previousBuckets)
  }

  private def mockedWindow(windowDuration: FiniteDuration, previousWindowDuration: FiniteDuration) = {
    val window = new CounterTimeWindow(windowDuration, previousWindowDuration) with CounterBucketStoreSupport with CounterSummaryStoreSupport with MetaSupport {
      override val bucketStore = mock[BucketStore[CounterBucket]](withSettings().verboseLogging())

      override val summaryStore = mock[SummaryStore[CounterSummary]](withSettings().verboseLogging())

      override val metaStore = mock[MetaStore](withSettings().verboseLogging())
    }
    window
  }

}