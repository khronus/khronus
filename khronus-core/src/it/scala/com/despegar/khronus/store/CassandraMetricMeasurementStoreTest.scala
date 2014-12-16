package com.despegar.khronus.store


import com.despegar.khronus.model._
import com.despegar.khronus.util.{JacksonJsonSupport, BaseIntegrationTest}
import org.scalatest.{Matchers, FunSuite}
import scala.concurrent.Await
import scala.concurrent.duration._

class CassandraMetricMeasurementStoreTest extends FunSuite with BaseIntegrationTest with Matchers with JacksonJsonSupport {
  override val tableNames: Seq[String] = CassandraHistogramBucketStore.windowDurations.map(duration => CassandraHistogramBucketStore.tableName(duration))

  test("A measurement should be stored") {
    val json = """  {"metrics":[{"name":"ultimo15","measurements":[{"ts":1418394322000,"values":[133]}],"mtype":"timer"},{"name":"r2d2_cpu_load_five","measurements":[{"ts":1417639706000,"values":[136]}],"mtype":"timer"},{"name":"r2d2_cpu_load_fifteen","measurements":[{"ts":1417639706000,"values":[112.00000000000001]}],"mtype":"timer"}]}  """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = CassandraHistogramBucketStore.slice(Metric("ultimo15",MetricType.Timer), 1, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.size should be (1)
    slice(0)._2().histogram.getTotalCount should be(1)
  }

  test("Negative values should be skipped") {
    val json = """ {"metrics":[{"name":"ultimo15","measurements":[{"ts":1418394322000,"values":[-9, -8, 133, -1, 150]}],"mtype":"timer"}]} """
    val batch:MetricBatch = mapper.readValue[MetricBatch](json)

    CassandraMetricMeasurementStore.storeMetricMeasurements(batch.metrics)

    Thread.sleep(500)

    val fslice = CassandraHistogramBucketStore.slice(Metric("ultimo15",MetricType.Timer), 1, System.currentTimeMillis(), 1 millis)
    val slice = Await.result(fslice, 1 second)

    slice.size should be (1)
    slice(0)._2().histogram.getTotalCount should be(2)
  }

}
