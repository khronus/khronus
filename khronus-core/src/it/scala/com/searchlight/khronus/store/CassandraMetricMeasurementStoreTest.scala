package com.searchlight.khronus.store

import com.searchlight.khronus.model._
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{Settings, JacksonJsonSupport, BaseIntegrationTest}
import org.scalatest.{Matchers, FunSuite}
import scala.concurrent.Await
import scala.concurrent.duration._

class CassandraMetricMeasurementStoreTest extends FunSuite with BaseIntegrationTest with Matchers with JacksonJsonSupport {
  override val tableNames: Seq[String] = Settings.Window.WindowDurations.map(duration => Buckets.histogramBucketStore.tableName(duration))

  test("A measurement should be stored") {
    val json = """  {"metrics":[{"name":"ultimo15","measurements":[{"ts":1418394322000,"values":[133]}],"mtype":"timer"},{"name":"r2d2_cpu_load_five","measurements":[{"ts":1417639706000,"values":[136]}],"mtype":"timer"},{"name":"r2d2_cpu_load_fifteen","measurements":[{"ts":1417639706000,"values":[112.00000000000001]}],"mtype":"timer"}]}  """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = Buckets.histogramBucketStore.slice(Metric("ultimo15",MetricType.Timer), 1, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.results.size should be (1)
    slice.results(0).lazyBucket().histogram.getTotalCount should be (1)
    slice.results(0).lazyBucket().histogram.getMaxValue should be (133)
  }

  test("Two measures in different storeGroupDuration should be store as two different histograms") {
    val ts1 = (1 second).toMillis
    val ts2 = ts1 + (6 seconds).toMillis
    val json = s"""  {"metrics":[{"name":"ultimo15","measurements":[{"ts":$ts1,"values":[133]}, {"ts":$ts2,"values":[183]}],"mtype":"timer"}]}  """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = Buckets.histogramBucketStore.slice(Metric("ultimo15",MetricType.Timer), 0, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.results.size should be (2)
    slice.results(0).lazyBucket().histogram.getTotalCount should be (1)
    slice.results(0).lazyBucket().histogram.getMaxValue should be (133)
    slice.results(1).lazyBucket().histogram.getTotalCount should be (1)
    slice.results(1).lazyBucket().histogram.getMaxValue should be (183)
  }

  test("Two measures in the same storeGroupDuration should be store as a single histogram") {
    val ts1 = (1 second).toMillis
    val ts2 = ts1 + (1 seconds).toMillis
    val json = s"""  {"metrics":[{"name":"ultimo19","measurements":[{"ts":$ts1,"values":[133]}, {"ts":$ts2,"values":[183]}],"mtype":"timer"}]}  """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = Buckets.histogramBucketStore.slice(Metric("ultimo19",MetricType.Timer), 0, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.results.size should be (1)
    slice.results(0).lazyBucket().histogram.getTotalCount should be (2)
    slice.results(0).lazyBucket().histogram.getMaxValue should be (183)
    slice.results(0).lazyBucket().histogram.getMinValue should be (133)
  }


  test("Negative values should be skipped") {
    val json = """ {"metrics":[{"name":"ultimo15","measurements":[{"ts":1418394322000,"values":[-9, -8, 133, -1, 150]}],"mtype":"timer"}]} """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = Buckets.histogramBucketStore.slice(Metric("ultimo15",MetricType.Timer), 1, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.results.size should be (1)
    slice.results(0).lazyBucket().histogram.getTotalCount should be(2)
  }

}
