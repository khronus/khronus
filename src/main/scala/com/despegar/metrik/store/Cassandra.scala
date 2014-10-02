package com.despegar.metrik.store

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

object Cassandra {

  private val context = new AstyanaxContext.Builder()
    .forCluster("MetrikCluster")
    .forKeyspace("metrik")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CassandraConnectionPool")
      .setPort(9160)
      .setMaxConnsPerHost(1)
      .setSeeds("127.0.0.1:9160"))
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance())

  context.start()
  
  val keyspace = context.getClient()
  
  def sliceUntilNow[T](columnFamily: String, key: String, columnMapper: Column[java.lang.Long] => T) = {
    val cf = ColumnFamily.newColumnFamily(columnFamily, StringSerializer.get(), LongSerializer.get())
    
    val result = keyspace.prepareQuery(cf).getKey(key).withColumnRange(1, System.currentTimeMillis(), false, 1000).execute()
    
    result.getResult().asScala.map( columnMapper ).toSeq
  }
  

}