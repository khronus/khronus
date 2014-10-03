package com.despegar.metrik.store

import com.despegar.metrik.model.HistogramBucket
import com.despegar.metrik.util.Config
import com.netflix.astyanax.Keyspace
import org.HdrHistogram.Histogram
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.duration._

class CassandraHistogramBucketStoreTest extends FunSuite with BeforeAndAfterAll with Config {

  var keyspace: Keyspace = null

  override def beforeAll = {
  }

  override def afterAll = {
  }


  test("An Histogram should be capable of serialize and deserialize from Cassandra") {
    val histograms = Seq(HistogramBucket(30, 30 seconds, new Histogram(3000000L, 3)))
    CassandraHistogramBucketStore.store("testMetric",30 seconds, histograms)
    assert(config.getString("cassandra.keyspace") == "metrikIntegrationTest")
  }

}
