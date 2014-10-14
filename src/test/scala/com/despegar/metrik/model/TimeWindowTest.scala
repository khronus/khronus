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

import com.despegar.metrik.store.{ StatisticSummaryStore, HistogramBucketStore }
import org.HdrHistogram.Histogram
import org.mockito.{ Matchers, ArgumentMatcher, ArgumentCaptor, Mockito }
import org.scalatest.{ FunSuite }
import org.scalatest.mock.MockitoSugar
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.despegar.metrik.store.HistogramBucketSupport
import com.despegar.metrik.store.StatisticSummarySupport

class TimeWindowTest extends FunSuite with MockitoSugar {

  private def getMockedWindow(windowDuration: FiniteDuration, previousWindowDuration: FiniteDuration) = {
    val window = new TimeWindow(windowDuration, previousWindowDuration) with HistogramBucketSupport with StatisticSummarySupport {
      override val histogramBucketStore = mock[HistogramBucketStore]

      override val statisticSummaryStore = mock[StatisticSummaryStore]
    }

    //Mockito.reset(window.histogramBucketStore, window.statisticSummaryStore)

    window
  }

  test("process 30 seconds window should store 2 buckets with their summary statistics") {
    val windowDuration: FiniteDuration = 30 seconds

    val window = getMockedWindow(windowDuration, 1 millis)

    //fill mocked histograms.
    val histogram1: Histogram = new Histogram(3000, 3)
    for (i ← 1 to 50) {
      histogram1.recordValue(i)
    }
    val histogram2: Histogram = new Histogram(3000, 3)
    for (i ← 51 to 100) {
      histogram2.recordValue(i)
    }
    val histogram3: Histogram = new Histogram(3000, 3)
    histogram3.recordValue(100L)
    histogram3.recordValue(100L)

    //make 2 buckets
    val bucket1: HistogramBucket = HistogramBucket(1, 1 millis, histogram1)
    val bucket2: HistogramBucket = HistogramBucket(2, 1 millis, histogram2)
    //second bucket
    val bucket3: HistogramBucket = HistogramBucket(30001, 1 millis, histogram3)

    val metricKey: String = "metrickA"

    //mock retrieve slice
    val histograms: Seq[HistogramBucket] = Seq(bucket1, bucket2, bucket3)
    Mockito.when(window.histogramBucketStore.sliceUntilNow(metricKey, 1 millis)).thenReturn(Future(histograms))

    //mock summaries
    Mockito.when(window.statisticSummaryStore.getLast(metricKey, windowDuration)).thenReturn(Future(Option(null)))

    //call method to test
    window.process(metricKey)

    val histogramBucketA: Histogram = Seq(bucket1, bucket2)
    val histogramBucketB: Histogram = Seq(bucket3)

    val summaryBucketA = StatisticSummary(0, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50.5)
    val summaryBucketB = StatisticSummary(30000, 100, 100, 100, 100, 100, 100, 100, 100, 2, 100)

    //verify the summaries for each bucket
    Mockito.verify(window.statisticSummaryStore).store(metricKey, windowDuration, Seq(summaryBucketB, summaryBucketA))

    //verify removal of previous buckets
    Mockito.verify(window.histogramBucketStore).remove("metrickA", 1 millis, Seq(bucket1, bucket2, bucket3))
  }

  test("RE-process 30 seconds window should not store any statistics") {
    val windowDuration: FiniteDuration = 30 seconds

    val window = getMockedWindow(windowDuration, 1 millis)

    //fill mocked histograms.
    val histogram1: Histogram = new Histogram(3000, 3)
    for (i ← 1 to 50) {
      histogram1.recordValue(i)
    }

    //make buckets
    val bucket1: HistogramBucket = HistogramBucket(15000, 1 millis, histogram1)
    val metricKey: String = "metrickA"

    //mock temporal data that for any reason was not deleted! (already processed)
    val histograms: Seq[HistogramBucket] = Seq(bucket1)
    Mockito.when(window.histogramBucketStore.sliceUntilNow(metricKey, 1 millis)).thenReturn(Future(histograms))

    //mock summary that match histogram from bucket1
    val summary = StatisticSummary(15000 * 1, 50, 80, 90, 95, 99, 100, 1, 100, 100, 50.5)
    Mockito.when(window.statisticSummaryStore.getLast(metricKey, windowDuration)).thenReturn(Future(Option(summary)))

    //call method to test
    window.process(metricKey)

    //verify that not store any temporal histogram
    //Mockito.verify(window.histogramBucketStore, Mockito.never()).store(metricKey, windowDuration, Seq())

    //verify that not store any summary
    Mockito.verify(window.statisticSummaryStore, Mockito.never()).store(metricKey, windowDuration, Seq())

    //verify removal of previous buckets
    Mockito.verify(window.histogramBucketStore).remove("metrickA", 1 millis, Seq(bucket1))
  }
}

