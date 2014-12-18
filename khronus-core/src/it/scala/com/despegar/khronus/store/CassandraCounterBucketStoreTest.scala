package com.despegar.khronus.store

import com.despegar.khronus.model.BucketNumber._
import com.despegar.khronus.model.Timestamp._
import com.despegar.khronus.model.{CounterBucket, Metric, MetricType}
import com.despegar.khronus.util.BaseIntegrationTest
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._


class CassandraCounterBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {
  override val tableNames: Seq[String] = Buckets.counterBucketStore.windowDurations.map(duration => Buckets.counterBucketStore.tableName(duration))

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
    val bucketFromCassandra = bucketsFromCassandra(0)

    counter shouldEqual bucketFromCassandra._2()

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

    bucketsFromCassandra should have length 1
    bucketsFromCassandra(0)._2() shouldEqual bucketFromThePast
  }

  test("should remove buckets") {

    val bucket1 = new CounterBucket((250L, 30 seconds), 200L)
    val bucket2 = new CounterBucket((250L, 30 seconds), 200L)

    await {
      Buckets.counterBucketStore.store(testMetric, 30 seconds, Seq(bucket1, bucket2))
    }

    val storedBuckets = await {
      Buckets.counterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds)
    }
    storedBuckets should have length 2

    await {
      Buckets.counterBucketStore.remove(testMetric, 30 seconds, storedBuckets.map(_._1))
    }
    val bucketsFromCassandra = await {
      Buckets.counterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds)
    }

    bucketsFromCassandra should be('empty)
  }

}
