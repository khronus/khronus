package com.despegar.metrik.store

import com.despegar.metrik.model.HistogramBucket
import com.despegar.metrik.util.Config
import com.netflix.astyanax.Keyspace
import org.HdrHistogram.Histogram
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.util.HashMap
import org.scalatest.Matchers
import scala.util.Random

class CassandraHistogramBucketStoreTest extends FunSuite with BeforeAndAfterAll with Config with Matchers {

  override def beforeAll = {
    createKeyspace
    createColumnFamilies
  }

  override def afterAll = dropKeyspace

  test("An Histogram should be capable of serialize and deserialize from Cassandra") {
    val histogram = HistogramBucket.newHistogram
    fill(histogram)
    val buckets = Seq(HistogramBucket(30, 30 seconds, histogram))
    CassandraHistogramBucketStore.store("testMetric", 30 seconds, buckets)
    val bucketsFromCassandra = CassandraHistogramBucketStore.sliceUntilNow("testMetric", 30 seconds)
    val bucketFromCassandra = bucketsFromCassandra(0)
    
    histogram shouldEqual bucketFromCassandra.histogram 
  }
  
  test("should not retrieve buckets from the future") {
    val histogram = HistogramBucket.newHistogram
    val futureBucket = System.currentTimeMillis() + 60000 / (30 seconds).toMillis
    val bucketFromTheFuture = HistogramBucket(futureBucket, 30 seconds, histogram)
    val bucketFromThePast = HistogramBucket(30, 30 seconds, histogram)
    
    val buckets = Seq(bucketFromThePast, bucketFromTheFuture)
    
    CassandraHistogramBucketStore.store("testMetric", 30 seconds, buckets)
    val bucketsFromCassandra = CassandraHistogramBucketStore.sliceUntilNow("testMetric", 30 seconds)
    
    bucketsFromCassandra.length shouldEqual 1
    bucketsFromCassandra(0) shouldEqual bucketFromThePast
  }
  
  private def fill(histogram: Histogram) = {
    (1 to 10000) foreach { i => histogram.recordValue(Random.nextInt(200)) }
  }

  private def createKeyspace = {
    val keyspace = Map("strategy_options" -> Map("replication_factor" -> "1").asJava, "strategy_class" -> "SimpleStrategy")
    val result = Cassandra.keyspace.createKeyspaceIfNotExists(keyspace.asJava).getResult();
    result.getSchemaId()
  }
  
  private def createColumnFamilies = {
    CassandraHistogramBucketStore.columnFamilies.values.foreach{ cf => 
    Cassandra.keyspace.createColumnFamily(cf, Map().asJava)
   } 
  }
  
  private def dropKeyspace = Cassandra.keyspace.dropKeyspace()
  
}
