package com.despegar.metrik.store

import com.despegar.metrik.model.{CounterBucket, Metric}
import com.despegar.metrik.util.BaseIntegrationTest
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.Try

class CassandraCounterBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  val testMetric = Metric("testMetric","counter")


  override def truncateColumnFamilies = Try {
    CassandraCounterBucketStore.columnFamilies foreach { cf =>
      Cassandra.keyspace.truncateColumnFamily(cf._2)
    }
  }

  test("should store and retrieve buckets properly") {
    val counter = new CounterBucket(250L, 30 seconds, 200L)
    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, Seq(counter)) }

    val executionTimestamp = counter.bucketNumber * counter.duration.toMillis
    val bucketsFromCassandra = await {CassandraCounterBucketStore.sliceUntil(testMetric, executionTimestamp, 30 seconds) }
    val bucketFromCassandra = bucketsFromCassandra(0)

    counter shouldEqual bucketFromCassandra
  }

  test("should not retrieve buckets from the future") {
    val futureBucket = (System.currentTimeMillis() + 60000) / (30 seconds).toMillis
    val bucketFromTheFuture = new CounterBucket(futureBucket, 30 seconds, 200L)
    val bucketFromThePast = new CounterBucket(250L, 30 seconds, 200L)

    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)

    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, buckets) }

    val bucketsFromCassandra = await { CassandraCounterBucketStore.sliceUntil(testMetric, System.currentTimeMillis(), 30 seconds) }

    bucketsFromCassandra should have length 1
    bucketsFromCassandra(0) shouldEqual bucketFromThePast
  }

  test("should remove buckets") {
    val bucket1 = new CounterBucket(250L, 30 seconds, 200L)
    val bucket2 = new CounterBucket(250L, 30 seconds, 200L)

    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, Seq(bucket1, bucket2)) }

    await { CassandraCounterBucketStore.remove(testMetric, 30 seconds, Seq(bucket1, bucket2)) }

    val bucketsFromCassandra = await { CassandraCounterBucketStore.sliceUntil(testMetric, System.currentTimeMillis(), 30 seconds) }

    bucketsFromCassandra should be ('empty)
  }
}
