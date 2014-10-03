package com.despegar.metrik.service

import org.scalatest.FunSuite
import org.specs2.mutable.Specification

class HistogramServiceTest extends FunSuite {

    test ("HistogramService  should return maximum time recorded for getMax") {
      val histo = new HistogramService
      histo.recordTime(2000)
      histo.recordTime(3000)
      histo.recordTime(1000)

      assert(histo.getMax() == 3000)
    }

    test ("HistogramService  should return 50% percentile time recorded for getPercentile") {
      val histo = new HistogramService
      histo.recordTime(2000)
      histo.recordTime(3000)
      histo.recordTime(1000)

      assert(histo.getPercentile(50) == 2000)
    }

    test("HistogramService  should ADD another histogram must join recorded times") {
      val histo = new HistogramService
      histo.recordTime(2000)
      histo.recordTime(3000)
      histo.recordTime(1000)

      val ahotherHisto = new HistogramService
      ahotherHisto.recordTime(1000)
      ahotherHisto.recordTime(3000)

      histo.add(ahotherHisto.histogram)

      assert(histo.getPercentile(50) == 2000)
    }
}

