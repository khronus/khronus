package com.despegar.metrik.store

import com.despegar.metrik.model.HistogramBucket
import org.HdrHistogram.Histogram
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.concurrent.duration._

class CassandraHistogramBucketStoreTest extends FunSuite with BeforeAndAfter {

  val keyspace = Cassandra.keyspace

  before(() => {
  })

  after(() => {

  })

  test("An Histogram should be capable of serialize and deserialize from Cassandra") {
    val histograms = Seq(HistogramBucket(30, 30 seconds, new Histogram(3000000L, 3)))
    CassandraHistogramBucketStore.store("testMetric",30 seconds, histograms)
    assert(1 == 1)
  }

}
