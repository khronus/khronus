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

package com.despegar.khronus.store

import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.despegar.khronus.model._
import com.despegar.khronus.util.log.Logging
import com.despegar.khronus.util.{ Measurable, Settings }
import org.HdrHistogram.{ Histogram, SkinnyHistogram }

import scala.concurrent.duration._

trait HistogramBucketSupport extends BucketStoreSupport[HistogramBucket] {
  override def bucketStore: BucketStore[HistogramBucket] = Buckets.histogramBucketStore
}

class CassandraHistogramBucketStore(session: Session) extends CassandraBucketStore[HistogramBucket](session) with Logging with Measurable {

  override def limit: Int = Settings.Histogram.BucketLimit

  override def fetchSize: Int = Settings.Histogram.BucketFetchSize

  private val histogramSerializer: HistogramSerializer = DefaultHistogramSerializer

  override def deserialize(windowDuration: Duration, timestamp: Long, histogram: Array[Byte]) = {
    new HistogramBucket(Timestamp(timestamp).toBucketNumberOf(windowDuration), deserializeHistogram(histogram))
  }

  override def tableName(duration: Duration) = s"histogramBucket${duration.length}${duration.unit}"

  def serialize(metric: Metric, windowDuration: Duration, bucket: HistogramBucket): ByteBuffer = {
    val buffer = histogramSerializer.serialize(bucket.histogram)
    val bytesEncoded = buffer.limit()
    log.trace(s"$metric- Histogram of $windowDuration with ${bucket.histogram.getTotalCount()} measures encoded and compressed into $bytesEncoded bytes")
    recordGauge(formatLabel("serializedBucketBytes", metric, windowDuration), bytesEncoded)
    buffer
  }

  private def deserializeHistogram(bytes: Array[Byte]): Histogram = histogramSerializer.deserialize(ByteBuffer.wrap(bytes))

  override def ttl(windowDuration: Duration): Int = Settings.Histogram.BucketRetentionPolicy

}

trait HistogramSerializer {
  def serialize(histogram: Histogram): ByteBuffer
  def deserialize(byteBuffer: ByteBuffer): Histogram
}

object DefaultHistogramSerializer extends HistogramSerializer {
  override def serialize(histogram: Histogram): ByteBuffer = {
    val buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = histogram.encodeIntoCompressedByteBuffer(buffer)
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
  }

  override def deserialize(buffer: ByteBuffer): Histogram = {
    SkinnyHistogram.decodeFromCompressedByteBuffer(buffer, 0)
  }
}