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

package com.searchlight.khronus.model

import java.io.IOException

import com.searchlight.khronus.model.BucketNumber._
import com.searchlight.khronus.model.Timestamp._
import com.searchlight.khronus.store._
import com.searchlight.khronus.util.MonitoringSupportMock
import org.HdrHistogram.Histogram
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NoStackTrace

class HistogramTimeWindowTest extends FunSuite with MockitoSugar with TimeWindowTest[HistogramBucket] {

  val metric = Metric("metrickA", MetricType.Timer)

  test("with previous buckets should store its buckets and summaries and remove previous buckets") {
    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket((1, previousWindowDuration), histogram1)
    val previousBucket2 = new HistogramBucket((2, previousWindowDuration), histogram2)
    val previousBucket3 = new HistogramBucket((30001, previousWindowDuration), histogram3)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = bucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val histogramA: Histogram = Seq(new HistogramBucket((1, previousWindowDuration), histogram1), new HistogramBucket((2, previousWindowDuration), histogram2))
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket((0, windowDuration), histogramA)
    val bucketB = new HistogramBucket((1, windowDuration), histogramB)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = HistogramSummary(0, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50)
    val summaryBucketB = HistogramSummary(30000, 100, 100, 100, 100, 100, 100, 100, 100, 2, 100)
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
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], Matchers.any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future(bucketSlice(Seq())))
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
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.slice(Matchers.eq(metric), any[Timestamp], Matchers.any[Timestamp], Matchers.eq(previousWindowDuration))).thenReturn(Future.failed(new IOException() with NoStackTrace))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future[Timestamp](neverProcessedTimestamp))

    //call method to test
    val tick = Tick(BucketNumber(15000, windowDuration))
    intercept[IOException] {
      Await.result(window.process(metric, tick), 5 seconds)
    }

    //verify that not store any temporal bucket
    verify(window.bucketStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[HistogramBucket]])

    //verify that not store any summary
    verify(window.summaryStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[HistogramSummary]])

    //verify that not remove anything
    //verify(window.bucketStore, never()).remove(any[Metric], any[FiniteDuration], any[Seq[Timestamp]])
  }

  test("with previous buckets should not remove them upon failure of temporal buckets store") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket((1, previousWindowDuration), histogram1)
    val previousBucket2 = new HistogramBucket((2, previousWindowDuration), histogram2)
    val previousBucket3 = new HistogramBucket((30001, previousWindowDuration), histogram3)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = bucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val histogramA: Histogram = Seq(previousBucket1, previousBucket2)
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket((0, windowDuration), histogramA)
    val bucketB = new HistogramBucket((1, windowDuration), histogramB)
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
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket((1, previousWindowDuration), histogram1)
    val previousBucket2 = new HistogramBucket((2, previousWindowDuration), histogram2)
    val previousBucket3 = new HistogramBucket((30001, previousWindowDuration), histogram3)
    val previousBuckets = lazyBuckets(Seq(previousBucket1, previousBucket2, previousBucket3))
    val uniqueTimestampsPreviousBuckets = uniqueTimestamps(Seq(previousBucket1, previousBucket2, previousBucket3))
    val previousBucketsMap = bucketSlice(uniqueTimestampsPreviousBuckets.zip(previousBuckets))

    val tick = Tick(previousBucket3.bucketNumber ~ windowDuration) //The last one

    val histogramA: Histogram = Seq(new HistogramBucket((1, previousWindowDuration), histogram1), new HistogramBucket((2, previousWindowDuration), histogram2))
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket((0, windowDuration), histogramA)
    val bucketB = new HistogramBucket((1, windowDuration), histogramB)
    val myBuckets = Seq(bucketA, bucketB)

    val summaryBucketA = HistogramSummary(0, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50)
    val summaryBucketB = HistogramSummary(30000, 100, 100, 100, 100, 100, 100, 100, 100, 2, 100)
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

  private def histogram1 = {
    val histogram1: Histogram = HistogramBucket.newHistogram(3000)
    for (i ← 1 to 50) {
      histogram1.recordValue(i)
    }
    histogram1
  }

  private def histogram2 = {
    val histogram2: Histogram = HistogramBucket.newHistogram(3000)
    for (i ← 51 to 100) {
      histogram2.recordValue(i)
    }
    histogram2
  }

  private def histogram3 = {
    val histogram3: Histogram = HistogramBucket.newHistogram(3000)
    histogram3.recordValue(100L)
    histogram3.recordValue(100L)
    histogram3
  }

  private def mockedWindow(windowDuration: FiniteDuration, previousWindowDuration: FiniteDuration) = {
    val window = new HistogramTimeWindow(windowDuration, previousWindowDuration) with HistogramBucketSupport with HistogramSummarySupport with MetaSupport with MonitoringSupportMock {
      override val bucketStore = mock[BucketStore[HistogramBucket]]

      override val summaryStore = mock[SummaryStore[HistogramSummary]]

      override val metaStore = mock[MetaStore]
    }
    window
  }
}