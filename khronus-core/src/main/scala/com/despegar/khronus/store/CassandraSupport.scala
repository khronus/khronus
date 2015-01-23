package com.despegar.khronus.store

import com.despegar.khronus.model.Monitoring
import com.despegar.khronus.util.ConcurrencySupport

trait CassandraSupport {
  val cassandraCluster = CassandraCluster
  val cassandraBuckets = Buckets
  val cassandraSummaries = Summaries
  val cassandraMeta = Meta

  cassandraMeta.metaStore.startSnapshotReloads()
  Monitoring.startMonitoringFlusher
  ConcurrencySupport.startConcurrencyMonitoring
}
