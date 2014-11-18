package com.despegar.metrik.store

import com.despegar.metrik.model.{UniqueTimestamp, CounterBucket, Metric}
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._
import com.despegar.metrik.model.Timestamp._
import com.despegar.metrik.model.BucketNumber._

class CassandraCounterBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  val testMetric = Metric("testMetric","counter")

  override def foreachColumnFamily(f: ColumnFamily[String,_] => OperationResult[_]) = {
    CassandraCounterBucketStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }

  test("should store and retrieve buckets properly") {
    val counter = new CounterBucket((250L, 30 seconds), 200L)
    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, Seq(counter)) }

    val executionTimestamp = counter.bucketNumber.startTimestamp()
    val bucketsFromCassandra = await {CassandraCounterBucketStore.slice(testMetric, 1, executionTimestamp, 30 seconds) }
    val bucketFromCassandra = bucketsFromCassandra(0)

    counter shouldEqual bucketFromCassandra._2()
  }

  test("should not retrieve buckets from the future") {
    val futureBucket = (System.currentTimeMillis() + 60000) / (30 seconds).toMillis
    val bucketFromTheFuture = new CounterBucket((futureBucket, 30 seconds), 200L)
    val bucketFromThePast = new CounterBucket((250L, 30 seconds), 200L)

    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)

    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, buckets) }

    val bucketsFromCassandra = await { CassandraCounterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds) }

    bucketsFromCassandra should have length 1
    bucketsFromCassandra(0)._2() shouldEqual bucketFromThePast
  }

  test("should remove buckets") {
    val bucket1 = new CounterBucket((250L, 30 seconds), 200L)
    val bucket2 = new CounterBucket((250L, 30 seconds), 200L)

    await { CassandraCounterBucketStore.store(testMetric, 30 seconds, Seq(bucket1, bucket2)) }

    val storedBuckets = await { CassandraCounterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds) }

    await { CassandraCounterBucketStore.remove(testMetric, 30 seconds, storedBuckets.map(_._1)   ) }

    val bucketsFromCassandra = await { CassandraCounterBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds) }

    bucketsFromCassandra should be ('empty)
  }
}
