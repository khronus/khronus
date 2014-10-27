package com.despegar.metrik.store

import java.nio.ByteBuffer

import com.despegar.metrik.model.{StatisticSummary, Bucket, Metric, CounterBucket}
import com.despegar.metrik.util.KryoSerializer
import com.netflix.astyanax.model.Column
import scala.concurrent.Future
import scala.concurrent.duration._

trait CounterBucketStoreSupport extends BucketStoreSupport[CounterBucket] {
  override def bucketStore: BucketStore[CounterBucket] = CassandraBucketStore
}

object CassandraBucketStore extends BucketStore[CounterBucket] {
  val windowDurations: Seq[Duration] = Seq(1 millis, 30 seconds) //FIXME put configured windows

  override def getColumnFamilyName(duration: Duration): String = ???

  override def toBucket(windowDuration: Duration)(column: Column[java.lang.Long]): CounterBucket = ???

  val serializer: KryoSerializer[CounterBucket] = new KryoSerializer("counterBucket", List(CounterBucket.getClass))

  override def serializeBucket(bucket: CounterBucket): ByteBuffer = {
    ByteBuffer.wrap(serializer.serialize(bucket))
  }
}
