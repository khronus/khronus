package com.despegar.metrik.store

import java.nio.ByteBuffer
import com.despegar.metrik.model.HistogramBucket
import scala.collection.JavaConverters._
import org.HdrHistogram.Histogram
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer
import scala.concurrent.duration._
import com.netflix.astyanax.ColumnListMutation

trait HistogramBucketStore {

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket]

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])
  
  def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])

}

object CassandraHistogramBucketStore extends HistogramBucketStore {
  //create column family definition for every bucket duration
  val windowDurations:Seq[Duration] = Seq(1 millis, 30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour) //FIXME put configured windows
  val columnFamilies = windowDurations.map(duration => (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap
  val LIMIT = 1000

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket] = {
    val result = Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(getKey(metric, windowDuration))
    								.withColumnRange(infinite, now, false, LIMIT).execute()
    result.getResult().asScala.map { column =>
      val timestamp = column.getName()
      val histogram = deserializeHistogram(column.getByteBufferValue)
      HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
    }.toSeq
  }

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) =>
      mutation.putColumn(bucket.timestamp, serializeHistogram(bucket.histogram))
    }
  }

  def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) =>
      mutation.deleteColumn(bucket.timestamp)
    }
  }

  private def mutate(metric:String, windowDuration:Duration, histogramBuckets: Seq[HistogramBucket])(f: (ColumnListMutation[java.lang.Long],HistogramBucket) => Unit) = {
    val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
    val mutation = mutationBatch.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
    histogramBuckets.foreach(f(mutation, _))
    mutationBatch.execute
  }
  
  private def getColumnFamilyName(duration: Duration) = s"bucket${duration.length}${duration.unit}"

  private def getKey(metric: String, windowDuration: Duration): String = s"$metric.${windowDuration.length}${windowDuration.unit}"

  private def serializeHistogram(histogram: Histogram): ByteBuffer = {
    val buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = histogram.encodeIntoCompressedByteBuffer(buffer)
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)
  
  private def now = System.currentTimeMillis()
  
  private def infinite = 1L

}