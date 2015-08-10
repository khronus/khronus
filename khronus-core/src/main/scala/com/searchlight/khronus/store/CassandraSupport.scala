package com.searchlight.khronus.store

import com.searchlight.khronus.util.ConcurrencySupport
import com.searchlight.khronus.util.log.Logging

trait CassandraSupport extends Logging {

  log.info("Initializing Cassandra")
  val cassandraCluster = CassandraCluster
  val cassandraBuckets = Buckets
  val cassandraSummaries = Summaries
  val cassandraMeta = Meta

  Meta.startReloads()
  ConcurrencySupport.startThreadPoolsMonitoring
}