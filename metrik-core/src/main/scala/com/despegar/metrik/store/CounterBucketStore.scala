/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.despegar.metrik.store

import java.nio.ByteBuffer
import com.despegar.metrik.model._
import com.despegar.metrik.util.KryoSerializer
import com.netflix.astyanax.model.Column
import scala.concurrent.Future
import scala.concurrent.duration._
import com.despegar.metrik.util.Settings
import com.despegar.metrik.model.Timestamp

trait CounterBucketStoreSupport extends BucketStoreSupport[CounterBucket] {
  override def bucketStore: BucketStore[CounterBucket] = CassandraCounterBucketStore
}

object CassandraCounterBucketStore extends BucketStore[CounterBucket] {

  val windowDurations: Seq[Duration] = Settings().Counter.WindowDurations

  val serializer: KryoSerializer[CounterBucket] = new KryoSerializer("counterBucket", List(CounterBucket.getClass))

  override def getColumnFamilyName(duration: Duration): String = s"counterBucket${duration.length}${duration.unit}"

  def deserialize(buffer: ByteBuffer): CounterBucket = {
    serializer.deserialize(buffer.array())
  }

  override def toBucket(windowDuration: Duration)(column: Column[UniqueTimestamp]): CounterBucket = {
    val uniqueTimestamp = column.getName()
    val counter = deserialize(column.getByteBufferValue)
    new CounterBucket(Timestamp(uniqueTimestamp.measurementTimestamp).toBucketNumber(windowDuration), counter.counts)
  }

  override def serializeBucket(metric: Metric, windowDuration: Duration, bucket: CounterBucket): ByteBuffer = {
    ByteBuffer.wrap(serializer.serialize(bucket))
  }

  override def ttl(windowDuration: Duration): Int = Settings().Counter.BucketRetentionPolicy
}
