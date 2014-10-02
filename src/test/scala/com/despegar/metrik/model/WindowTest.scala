package com.despegar.metrik.model

import com.despegar.metrik.store.HistogramBucketStore
import org.HdrHistogram.Histogram
import org.mockito.Mockito
import org.scalatest.FlatSpec
import org.scalatest.mock.MockitoSugar
import org.specs2.mutable.Specification

import scala.concurrent.duration._

class WindowTest extends FlatSpec with MockitoSugar {

  "A Window" should "lalala" in {
        val window = new Window(30 seconds, 0 seconds) with HistogramBucketSupport { override val histogramBucketStore = mock[HistogramBucketStore]}

        val histograms: Seq[HistogramBucket] = Seq(HistogramBucket(1,30 seconds, new Histogram(3000,3) ))

        Mockito.when(window.histogramBucketStore.sliceUntilNow("metrickA",0 seconds)).thenReturn(histograms)

        val mockedHistograms = window.histogramBucketStore.sliceUntilNow("metrickA",0 seconds)

        assert(mockedHistograms != null)
  }
}
