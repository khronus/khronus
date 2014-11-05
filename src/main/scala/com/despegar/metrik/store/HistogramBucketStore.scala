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
import java.util.concurrent.{ TimeUnit, Executors }
import com.despegar.metrik.model.HistogramBucket
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.Column
import org.HdrHistogram.Histogram
import scala.concurrent.duration._
import com.despegar.metrik.util.Settings
import com.despegar.metrik.model.Metric

trait HistogramBucketSupport extends BucketStoreSupport[HistogramBucket] {
  override def bucketStore: BucketStore[HistogramBucket] = CassandraHistogramBucketStore
}

object CassandraHistogramBucketStore extends BucketStore[HistogramBucket] with Logging {

  val windowDurations: Seq[Duration] = Settings().Histogram.windowDurations

  override def toBucket(windowDuration: Duration)(column: Column[java.lang.Long]) = {
    val timestamp = column.getName()
    val histogram = deserializeHistogram(column.getByteBufferValue)
    new HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
  }

  override def getColumnFamilyName(duration: Duration) = s"histogramBucket${duration.length}${duration.unit}"

  def serializeBucket(metric: Metric, windowDuration: Duration, bucket: HistogramBucket): ByteBuffer = {
    val buffer = ByteBuffer.allocate(bucket.histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = bucket.histogram.encodeIntoCompressedByteBuffer(buffer) //TODO: Find a better way to do this serialization
    log.debug(s"$metric- Histogram of $windowDuration with ${bucket.histogram.getTotalCount()} measures encoded and compressed into $bytesEncoded bytes")
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)

}