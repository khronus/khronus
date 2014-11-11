package com.despegar.metrik.store

import com.despegar.metrik.model.HistogramBucket
import com.despegar.metrik.util.BaseIntegrationTest
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import org.HdrHistogram.Histogram
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._
import scala.util.Random
import com.despegar.metrik.model.Metric
import com.despegar.metrik.model.Timestamp._
import com.despegar.metrik.model.BucketNumber._
import com.despegar.metrik.model.Timestamp

class CassandraHistogramBucketStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  val testMetric = Metric("testMetric","histogram")
  
  test("should store and retrieve buckets properly") {
    val histogram = HistogramBucket.newHistogram
    fill(histogram)
    val histogramBucket = new HistogramBucket((30, 30 seconds), histogram)
    await { CassandraHistogramBucketStore.store(testMetric, 30 seconds, Seq(histogramBucket)) }

    val executionTimestamp:Timestamp = histogramBucket.bucketNumber.startTimestamp()
    val bucketsFromCassandra = await {CassandraHistogramBucketStore.slice(testMetric, 1, executionTimestamp, 30 seconds) }
    val bucketFromCassandra = bucketsFromCassandra(0)

    histogram shouldEqual bucketFromCassandra.histogram
  }
  
  test("should not retrieve buckets from the future") {
    val histogram = HistogramBucket.newHistogram
    val futureBucket = System.currentTimeMillis() + 60000 / (30 seconds).toMillis
    val bucketFromTheFuture = new HistogramBucket((futureBucket, 30 seconds), histogram)
    val bucketFromThePast = new HistogramBucket((30, 30 seconds), histogram)
    
    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)
    
    await { CassandraHistogramBucketStore.store(testMetric, 30 seconds, buckets) }
    
    val bucketsFromCassandra = await { CassandraHistogramBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds) }
    
    bucketsFromCassandra should have length 1
    bucketsFromCassandra(0) shouldEqual bucketFromThePast
  }
  
  test("should remove buckets") {
    val bucket1 = new HistogramBucket((1, 30 seconds), HistogramBucket.newHistogram)
    val bucket2 = new HistogramBucket((2, 30 seconds), HistogramBucket.newHistogram)
    
    await { CassandraHistogramBucketStore.store(testMetric, 30 seconds, Seq(bucket1, bucket2)) }
    
    await { CassandraHistogramBucketStore.remove(testMetric, 30 seconds, Seq(bucket1, bucket2)) }

    val bucketsFromCassandra = await { CassandraHistogramBucketStore.slice(testMetric, 1, System.currentTimeMillis(), 30 seconds) }
    
    bucketsFromCassandra should be ('empty)
  }
  
  private def fill(histogram: Histogram) = {
    (1 to 10000) foreach { i => histogram.recordValue(Random.nextInt(200)) }
  }

  override def foreachColumnFamily(f: ColumnFamily[String,java.lang.Long] => OperationResult[_]) = {
    CassandraHistogramBucketStore.columnFamilies.values.foreach{ cf => val or = f(cf); or.getResult }
  }

}
