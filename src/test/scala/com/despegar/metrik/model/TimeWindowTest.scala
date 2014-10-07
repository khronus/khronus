package com.despegar.metrik.model

import com.despegar.metrik.store.{StatisticSummaryStore, HistogramBucketStore}
import org.HdrHistogram.Histogram
import org.mockito.Mockito
import org.scalatest.{FunSuite, FlatSpec}
import org.scalatest.mock.MockitoSugar
import org.specs2.mutable.Specification

import scala.concurrent.duration._

class TimeWindowTest extends FunSuite with MockitoSugar {

  test("process 30 seconds window should store 2 buckets with their summary statistics") {
    val windowDuration: FiniteDuration = 30 seconds

    val window = new TimeWindow(windowDuration, 1 millis) with HistogramBucketSupport with StatisticSummarySupport {
      override val histogramBucketStore = mock[HistogramBucketStore]

      override val statisticSummaryStore = mock[StatisticSummaryStore]
    }

    //fill mocked histograms.
    val histogram1: Histogram = new Histogram(3000, 3)
    for (i <- 1 to 50) {
      histogram1.recordValue(i)
    }
    val histogram2: Histogram = new Histogram(3000, 3)
    for (i <- 51 to 100) {
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
    Mockito.when(window.histogramBucketStore.sliceUntilNow(metricKey, 1 millis)).thenReturn(histograms)

    //call method to test
    window.process(metricKey)

    val histogramBucketA : Histogram = Seq(bucket1, bucket2)
    val histogramBucketB: Histogram = Seq(bucket3)

    //verify that two buckets were stored, and their histograms where summ
    Mockito.verify(window.histogramBucketStore).store(metricKey, windowDuration, Seq(HistogramBucket(1, windowDuration, histogramBucketB), HistogramBucket(0, windowDuration, histogramBucketA)))

    val summaryBucketA = StatisticSummary(0, 50,80,90,95,99,100,1,100,100,50.5)
    val summaryBucketB = StatisticSummary(30000, 100,100,100,100,100,100,100,100,2,100)

    //verify the summaries for each bucket
    Mockito.verify(window.statisticSummaryStore).store(metricKey, windowDuration, Seq(summaryBucketB, summaryBucketA))
    
    //verify removal of previous buckets
    Mockito.verify(window.histogramBucketStore).remove("metrickA", 1 millis, Seq(bucket1, bucket2, bucket3))
  }
}
