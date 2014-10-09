package com.despegar.metrik.store

import com.despegar.metrik.util.Config
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer
import scala.collection.JavaConverters._
import com.netflix.astyanax.model.Column
import scala.util.Try
import com.despegar.metrik.util.Logging

object Cassandra extends Config with Logging {

  private val context = new AstyanaxContext.Builder()
    .forCluster(config.getString("cassandra.cluster"))
    .forKeyspace(config.getString("cassandra.keyspace"))
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CassandraConnectionPool")
      .setPort(config.getInt("cassandra.port"))
      .setMaxConnsPerHost(1)
      .setSeeds(config.getString("cassandra.seeds")))
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance())

  context.start()

  val keyspace = context.getClient()

  def initialize = {
    initializeKeyspace
    CassandraMetaStore.initialize
    CassandraHistogramBucketStore.initialize
    CassandraStatisticSummaryStore.initialize
  }

  private def initializeKeyspace = {
    Try {
      log.info("Initializing metrik keyspace...")
      keyspace.createKeyspaceIfNotExists(
        Map("strategy_options" -> Map("replication_factor" -> "1").asJava, "strategy_class" -> "SimpleStrategy").asJava)
        .getResult();
    }
  }

  def createColumnFamily[T, U](columnFamily: ColumnFamily[T, U]) = Try {
    log.info(s"Initializing columnFamily[${columnFamily.getName()}]...")
    keyspace.createColumnFamily(columnFamily, Map[String, Object]().asJava)
    log.info(s"columnFamily[${columnFamily.getName()}] created successfully")
  }

}