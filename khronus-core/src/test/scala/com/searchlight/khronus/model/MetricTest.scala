package com.searchlight.khronus.model

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, FunSuite }

class MetricTest extends FunSuite with Matchers with MockitoSugar {

  private val mtype: MetricType = "counter"
  private val flatNameWithTags = "~system.emptySliceTime.timer.1HOURS[tag1:value1,tag2:value2,tag3:value3]"
  private val flatNameWithoutTags = "~system.emptySliceTime.timer.1HOURS"
  private val appFlatNameWithTags = "testApp:emptySliceTime.timer.1HOURS[tag1:value1,tag2:value2,tag3:value3]"

  private val metricWithTags = Metric("~system.emptySliceTime.timer.1HOURS", mtype, Map("tag1" -> "value1", "tag2" -> "value2", "tag3" -> "value3"))
  private val metricWithoutTags = Metric("~system.emptySliceTime.timer.1HOURS", mtype, Map())
  private val appMetricWithTags = Metric("testApp:emptySliceTime.timer.1HOURS", mtype, Map("tag1" -> "value1", "tag2" -> "value2", "tag3" -> "value3"))

  test("App like flatName to metric") {
    Metric.fromFlatNameToMetric(appFlatNameWithTags, mtype) should equal(appMetricWithTags)
  }

  test("flatName to metric") {
    Metric.fromFlatNameToMetric(flatNameWithTags, mtype) should equal(metricWithTags)
  }

  test("flatName with symbols to metric") {
    val nameWithSymbols = "${azd-host}:BookingManager.svc:status:503"
    Metric.fromFlatNameToMetric(nameWithSymbols, mtype) should equal(Metric(nameWithSymbols, mtype, Map()))
  }

  test("flatName to metric without tags") {
    Metric.fromFlatNameToMetric(flatNameWithoutTags, mtype) should equal(metricWithoutTags)
  }

  test("metric flatName") {
    metricWithTags.flatName should equal(flatNameWithTags)
  }

  test("metric without tags flatName") {
    metricWithoutTags.flatName should equal(flatNameWithoutTags)
  }

}
