package com.despegar.khronus.store

import com.despegar.khronus.model.BucketNumber._
import com.despegar.khronus.model.Timestamp._
import com.despegar.khronus.model.{CounterBucket, Metric, MetricType}
import com.despegar.khronus.util.{Settings, BaseIntegrationTest}
import org.scalatest.{FunSuite, Matchers}
import com.despegar.khronus.model.{Metric, MetricType, CounterBucket}

import scala.concurrent.duration._


class CassandraCounterBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {
  override val tableNames: Seq[String] = Settings.Window.WindowDurations.map(duration => Buckets.counterBucketStore.tableName(duration))

  val testMetric = Metric("testMetric", MetricType.Counter)

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  test("should store and retrieve buckets properly") {
    val counter = new CounterBucket((250L, 30 seconds), 200L)
    await {
      Buckets.counterBucketStore.store(testMetric, 30 seconds, Seq(counter))
    }

    val executionTimestamp = counter.bucketNumber.startTimestamp()
    val bucketsFromCassandra = await {
      Buckets.counterBucketStore.slice(testMetric, 1, executionTimestamp, 30 seconds)
    }
    val bucketFromCassandra = bucketsFromCassandra.results(0)

    counter shouldEqual bucketFromCassandra.lazyBucket()

  }

  test("should not retrieve buckets from the future") {
    val futureBucket = (System.currentTimeMillis() + 60000) / (30 seconds).toMillis
    val bucketFromTheFuture = new CounterBucket((futureBucket, 30 seconds), 200L)
    val bucketFromThePast = new CounterBucket((250L, 30 seconds), 200L)

    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)

    await {
      Buckets.counterBucketStore.store(testMetric, 30 seconds, buckets)
    }
    val bucketsFromCassandra = await {
      Buckets.counterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds)
    }

    bucketsFromCassandra.results should have length 1
    bucketsFromCassandra.results(0).lazyBucket() shouldEqual bucketFromThePast
  }

}
