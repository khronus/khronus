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

class HistogramTimeWindowTest extends FunSuite with MockitoSugar {

  val metric = Metric("metrickA", "histogram")


  test("with previous buckets should store its buckets and summaries and remove previous buckets") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket(1, previousWindowDuration, histogram1)
    val previousBucket2 = new HistogramBucket(2, previousWindowDuration, histogram2)
    val previousBucket3 = new HistogramBucket(30001, previousWindowDuration, histogram3)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val executionTime = previousBucket3.timestamp //The last one

    val histogramA: Histogram = Seq(previousBucket1, previousBucket2)
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket(0, windowDuration, histogramA)
    val bucketB = new HistogramBucket(1, windowDuration, histogramB)
    val myBuckets = Seq(bucketB, bucketA)

    val summaryBucketA = StatisticSummary(0, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50)
    val summaryBucketB = StatisticSummary(30000, 100, 100, 100, 100, 100, 100, 100, 100, 2, 100)
    val mySummaries = Seq(summaryBucketB, summaryBucketA)

    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(-Long.MaxValue))
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, previousBuckets)).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, executionTime), 5 seconds)

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify summary statistics store
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets removal
    verify(window.bucketStore).remove(metric, previousWindowDuration, previousBuckets)
  }

  test("1 minute window should increment last bucket") {
    val windowDuration: FiniteDuration = 1 minute
    val previousWindowDuration: FiniteDuration = 30 seconds

    val window = mockedWindow(windowDuration, previousWindowDuration)
    val executionTime = 1415205230052L + 30000L //The last one

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(1415205210000L))

    val previousBucket1 = new HistogramBucket(47173507L, previousWindowDuration, histogram1)
    val previousBuckets = Seq(previousBucket1)
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(Matchers.eq(metric), Matchers.eq(windowDuration), any[Seq[HistogramBucket]])).thenReturn(Future {})
    when(window.summaryStore.store(Matchers.eq(metric), Matchers.eq(windowDuration), any[Seq[StatisticSummary]])).thenReturn(Future {})
    when(window.bucketStore.remove(Matchers.eq(metric), Matchers.eq(previousWindowDuration), any[Seq[HistogramBucket]])).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, executionTime), 5 seconds)

    //verify that not store any temporal histogram
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify removal of previous undeleted buckets
    verify(window.bucketStore).remove(metric, previousWindowDuration, previousBuckets)
  }

  test("with already processed buckets should remove them without storing any bucket or summary") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data that for any reason was not deleted! (already processed)
    val somePreviousBucket = new HistogramBucket(15000, previousWindowDuration, histogram1)
    val previousUndeletedBuckets = Seq(somePreviousBucket)
    val executionTime = somePreviousBucket.timestamp

    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousUndeletedBuckets))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(60000L))
    when(window.bucketStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, previousUndeletedBuckets)).thenReturn(Future {})

    //call method to test
    Await.result(window.process(metric, executionTime), 5 seconds)

    //verify that not store any temporal histogram
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify removal of previous undeleted buckets
    verify(window.bucketStore).remove(metric, previousWindowDuration, previousUndeletedBuckets)
  }

  test("without previous buckets should do nothing") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(Seq()))
    when(window.bucketStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, Seq())).thenReturn(Future {})
    when(window.bucketStore.remove(metric, previousWindowDuration, Seq())).thenReturn(Future {})
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(-Long.MaxValue))

    //call method to test
    Await.result(window.process(metric, System.currentTimeMillis()), 5 seconds)

    //verify that not store any temporal bucket
    verify(window.bucketStore).store(metric, windowDuration, Seq())

    //verify that not store any summary
    verify(window.summaryStore).store(metric, windowDuration, Seq())

    //verify that not remove anything
    verify(window.bucketStore).remove(metric, previousWindowDuration, Seq())
  }

  test("should do nothing upon failure of previous buckets slice retrieval") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    //mock temporal data to be empty
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), Matchers.any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future.failed(new IOException()))
    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(-Long.MaxValue))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, System.currentTimeMillis()), 5 seconds)
    }

    //verify that not store any temporal bucket
    verify(window.bucketStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[HistogramBucket]])

    //verify that not store any summary
    verify(window.summaryStore, never()).store(any[Metric], any[FiniteDuration], any[Seq[StatisticSummary]])

    //verify that not remove anything
    verify(window.bucketStore, never()).remove(any[Metric], any[FiniteDuration], any[Seq[HistogramBucket]])
  }

  test("with previous buckets should not remove them upon failure of temporal buckets store") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket(1, previousWindowDuration, histogram1)
    val previousBucket2 = new HistogramBucket(2, previousWindowDuration, histogram2)
    val previousBucket3 = new HistogramBucket(30001, previousWindowDuration, histogram3)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val executionTime = previousBucket3.timestamp //The last one

    val histogramA: Histogram = Seq(previousBucket1, previousBucket2)
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket(0, windowDuration, histogramA)
    val bucketB = new HistogramBucket(1, windowDuration, histogramB)
    val myBuckets = Seq(bucketB, bucketA)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(-Long.MaxValue))
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future.failed(new IOException()))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, executionTime), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.bucketStore).store(metric, windowDuration, myBuckets)

    //verify previous buckets were not removed
    verify(window.bucketStore, never()).remove(metric, previousWindowDuration, previousBuckets)
  }

  test("with previous buckets should not remove them upon failure of summaries store") {
    val windowDuration: FiniteDuration = 30 seconds
    val previousWindowDuration: FiniteDuration = 1 millis

    val window = mockedWindow(windowDuration, previousWindowDuration)

    val previousBucket1 = new HistogramBucket(1, previousWindowDuration, histogram1)
    val previousBucket2 = new HistogramBucket(2, previousWindowDuration, histogram2)
    val previousBucket3 = new HistogramBucket(30001, previousWindowDuration, histogram3)
    val previousBuckets = Seq(previousBucket1, previousBucket2, previousBucket3)

    val executionTime = previousBucket3.timestamp //The last one

    val histogramA: Histogram = Seq(previousBucket1, previousBucket2)
    val histogramB: Histogram = Seq(previousBucket3)

    val bucketA = new HistogramBucket(0, windowDuration, histogramA)
    val bucketB = new HistogramBucket(1, windowDuration, histogramB)
    val myBuckets = Seq(bucketB, bucketA)

    val summaryBucketA = StatisticSummary(0, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50)
    val summaryBucketB = StatisticSummary(30000, 100, 100, 100, 100, 100, 100, 100, 100, 2, 100)
    val mySummaries = Seq(summaryBucketB, summaryBucketA)

    when(window.metaStore.getLastProcessedTimestamp(metric)).thenReturn(Future(-Long.MaxValue))
    when(window.bucketStore.sliceUntil(Matchers.eq(metric), any[Long], Matchers.eq(previousWindowDuration))).thenReturn(Future(previousBuckets))
    when(window.bucketStore.store(metric, windowDuration, myBuckets)).thenReturn(Future {})
    when(window.summaryStore.store(metric, windowDuration, mySummaries)).thenReturn(Future.failed(new IOException()))

    //call method to test
    intercept[IOException] {
      Await.result(window.process(metric, executionTime), 5 seconds)
    }

    //verify temporal buckets store for subsequent window
    verify(window.summaryStore).store(metric, windowDuration, mySummaries)

    //verify previous buckets were not removed
    verify(window.bucketStore, never()).remove(metric, previousWindowDuration, previousBuckets)
  }

  private def histogram1 = {
    val histogram1: Histogram = new Histogram(3000, 3)
    for (i ← 1 to 50) {
      histogram1.recordValue(i)
    }
    histogram1
  }

  private def histogram2 = {
    val histogram2: Histogram = new Histogram(3000, 3)
    for (i ← 51 to 100) {
      histogram2.recordValue(i)
    }
    histogram2
  }

  private def histogram3 = {
    val histogram3: Histogram = new Histogram(3000, 3)
    histogram3.recordValue(100L)
    histogram3.recordValue(100L)
    histogram3
  }

  private def mockedWindow(windowDuration: FiniteDuration, previousWindowDuration: FiniteDuration) = {
    val window = new HistogramTimeWindow(windowDuration, previousWindowDuration) with HistogramBucketSupport with StatisticSummarySupport with MetaSupport {
      override val bucketStore = mock[BucketStore[HistogramBucket]]

      override val summaryStore = mock[SummaryStore[StatisticSummary]]

      override val metaStore = mock[MetaStore]
    }
    window
  }
}

