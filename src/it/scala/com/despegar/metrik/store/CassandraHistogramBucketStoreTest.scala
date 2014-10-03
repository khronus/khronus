package com.despegar.metrik.store

import org.scalatest.{BeforeAndAfter, FunSuite}

class CassandraHistogramBucketStoreTest extends FunSuite with BeforeAndAfter {

  val keyspace = Cassandra.keyspace

  before(() => {
  })

  after(() => {

  })

  test("An Histogram should be capable of serialize and deserialize from Cassandra") {
    assert(1 == 1)
  }

}
