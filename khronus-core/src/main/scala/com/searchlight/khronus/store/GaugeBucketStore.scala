/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.searchlight.khronus.store

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.searchlight.khronus.model.bucket.GaugeBucket
import com.searchlight.khronus.model.{ Timestamp, _ }
import com.searchlight.khronus.util.{ Measurable, Settings }
import com.esotericsoftware.kryo.io.{ Input, Output }

import scala.concurrent.duration._

trait GaugeBucketStoreSupport extends BucketStoreSupport[GaugeBucket] {
  override def bucketStore: BucketStore[GaugeBucket] = Buckets.gaugeBucketStore
}

class CassandraGaugeBucketStore(session: Session) extends CassandraBucketStore[GaugeBucket](session) with Measurable {

  override def limit: Int = Settings.Gauges.BucketLimit

  override def fetchSize: Int = Settings.Gauges.BucketFetchSize

  override def tableName(duration: Duration): String = s"gaugeBucket${duration.length}${duration.unit}"

  private val serializer: GaugeBucketSerializer = DefaultGaugeBucketSerializer

  override def deserialize(windowDuration: Duration, timestamp: Long, gauge: Array[Byte]) = {
    serializer.deserialize(Timestamp(timestamp).toBucketNumberOf(windowDuration), gauge)
  }

  override def serialize(metric: Metric, windowDuration: Duration, bucket: Bucket): ByteBuffer = {
    val buffer = serializer.serialize(bucket.asInstanceOf[GaugeBucket])
    recordGauge(formatLabel("serializedBucketBytes", metric, windowDuration), buffer.array().length)
    buffer
  }

  override def ttl(windowDuration: Duration): Int = Settings.Gauges.BucketRetentionPolicy
}

trait GaugeBucketSerializer {
  def serialize(bucket: GaugeBucket): ByteBuffer

  def deserialize(bucketNumber: BucketNumber, buffer: Array[Byte]): GaugeBucket
}

object DefaultGaugeBucketSerializer extends GaugeBucketSerializer {
  override def serialize(bucket: GaugeBucket): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    output.writeByte(1)
    output.writeVarLong(bucket.min, true)
    output.writeVarLong(bucket.max, true)
    output.writeVarLong(bucket.average, true)
    output.writeVarLong(bucket.count, true)
    output.flush()
    baos.flush()
    output.close()
    val array: Array[Byte] = baos.toByteArray
    val buffer = ByteBuffer.wrap(array)
    output.close()
    buffer
  }

  override def deserialize(bucketNumber: BucketNumber, buffer: Array[Byte]): GaugeBucket = {
    val input = new Input(buffer)
    val version = input.readByte()
    if (version == 1) {
      //TODO: versioned
      val min = input.readVarLong(true)
      val max = input.readVarLong(true)
      val average = input.readVarLong(true)
      val count = input.readVarLong(true)
      input.close()
      GaugeBucket(bucketNumber, min, max, average, count)
    } else GaugeBucket(bucketNumber, 0, 0, 0, 0)
  }
}
