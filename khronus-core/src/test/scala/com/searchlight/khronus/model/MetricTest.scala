package com.searchlight.khronus.model

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, FunSuite }

class MetricTest extends FunSuite with Matchers with MockitoSugar {

  private val metricWithTags = Metric("someMetric[tag1:value1,tag2:value2,tag3:value3]", "counter")
  private val metricWithoutTags = Metric("someMetric", "counter")
  private val subMetricWithTags = SubMetric(Metric("someMetric", "counter"),
    Map("tag1" -> "value1", "tag2" -> "value2", "tag3" -> "value3"))
  private val subMetricWithoutTags = SubMetric(Metric("someMetric", "counter"), Map())

  test("metric to submetric") {
    metricWithTags.asSubMetric() should equal(subMetricWithTags)
  }

  test("metric without tags to submetric") {
    metricWithoutTags.asSubMetric() should equal(subMetricWithoutTags)
  }

  test("submetric to metric") {
    subMetricWithTags.asMetric() should equal(metricWithTags)
  }

  test("submetric without tags to metric") {
    subMetricWithoutTags.asMetric() should equal(metricWithoutTags)
  }

}
