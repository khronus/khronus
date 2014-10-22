package com.despegar.metrik.store

import com.despegar.metrik.model.HistogramBucket
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.HdrHistogram.Histogram
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.Random

class CassandraHistogramBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  test("should store and retrieve buckets properly") {
    val histogram = HistogramBucket.newHistogram
    fill(histogram)
    val histogramBucket = HistogramBucket(30, 30 seconds, histogram)
    await { CassandraHistogramBucketStore.store("testMetric", 30 seconds, Seq(histogramBucket)) }

    val executionTimestamp = histogramBucket.bucketNumber * histogramBucket.duration.toMillis
    val bucketsFromCassandra = await {CassandraHistogramBucketStore.sliceUntil("testMetric", executionTimestamp, 30 seconds) }
    val bucketFromCassandra = bucketsFromCassandra(0)

    histogram shouldEqual bucketFromCassandra.histogram
  }
  
  test("should not retrieve buckets from the future") {
    val histogram = HistogramBucket.newHistogram
    val futureBucket = System.currentTimeMillis() + 60000 / (30 seconds).toMillis
    val bucketFromTheFuture = HistogramBucket(futureBucket, 30 seconds, histogram)
    val bucketFromThePast = HistogramBucket(30, 30 seconds, histogram)
    
    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)
    
    await { CassandraHistogramBucketStore.store("testMetric", 30 seconds, buckets) }
    
    val bucketsFromCassandra = await { CassandraHistogramBucketStore.sliceUntil("testMetric", System.currentTimeMillis(), 30 seconds) }
    
    bucketsFromCassandra should have length 1
    bucketsFromCassandra(0) shouldEqual bucketFromThePast
  }
  
  test("should remove buckets") {
    val bucket1 = HistogramBucket(1, 30 seconds, HistogramBucket.newHistogram)
    val bucket2 = HistogramBucket(2, 30 seconds, HistogramBucket.newHistogram)
    
    await { CassandraHistogramBucketStore.store("testMetric", 30 seconds, Seq(bucket1, bucket2)) }
    
    await { CassandraHistogramBucketStore.remove("testMetric", 30 seconds, Seq(bucket1, bucket2)) }

    val bucketsFromCassandra = await { CassandraHistogramBucketStore.sliceUntil("testMetric", System.currentTimeMillis(), 30 seconds) }
    
    bucketsFromCassandra should be ('empty)
  }
  
  private def fill(histogram: Histogram) = {
    (1 to 10000) foreach { i => histogram.recordValue(Random.nextInt(200)) }
  }

  override def foreachColumnFamily(f: ColumnFamily[String,java.lang.Long] => OperationResult[_]) = {
    CassandraHistogramBucketStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }

}
