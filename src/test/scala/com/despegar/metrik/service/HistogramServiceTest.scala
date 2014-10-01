package com.despegar.metrik.service

import org.specs2.mutable.Specification

/**
 * Created by dberjman on 10/1/14.
 */
class HistogramServiceTest extends Specification {

  "HistogramService" should {

    "return maximum time recorded for getMax" in {
      val histo = new HistogramService
      histo.recordTime(2000)
      histo.recordTime(3000)
      histo.recordTime(1000)

      histo.getMax() mustEqual(3000)
    }

    "return 50% percentile time recorded for getPercentile" in {
      val histo = new HistogramService
      histo.recordTime(2000)
      histo.recordTime(3000)
      histo.recordTime(1000)

      histo.getPercentile(50) mustEqual(2000)
    }
  }
}
