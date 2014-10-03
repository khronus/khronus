package com.despegar.metrik.store

import java.nio.ByteBuffer

import scala.concurrent.duration.Duration
import com.despegar.metrik.model.HistogramBucket
import scala.collection.JavaConverters._
import org.HdrHistogram.Histogram
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer

trait HistogramBucketStore {

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket]

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])

}

object CassandraHistogramBucketStore extends HistogramBucketStore {

  val columnFamily = ColumnFamily.newColumnFamily("histogramBucket", StringSerializer.get(), LongSerializer.get())
  val LIMIT = 1000

  def sliceUntilNow(metric: String, windowDuration: Duration): Seq[HistogramBucket] = {
    val result = Cassandra.keyspace.prepareQuery(columnFamily).getKey(getKey(metric, windowDuration)).withColumnRange(1L, System.currentTimeMillis(), false, LIMIT).execute()

    result.getResult().asScala.map { column =>
      val timestamp = column.getName()
      val histogram = deserializeHistogram(column.getByteBufferValue)
      HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
    }.toSeq
    
  }


  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    val mutation = Cassandra.keyspace.prepareMutationBatch()
    val colums = mutation.withRow(columnFamily, getKey(metric, windowDuration))
    histogramBuckets.foreach( bucket => colums.putColumn(bucket.timestamp, serializeHistogram(bucket.histogram)))

    mutation.execute
  }

  def getKey(metric: String, windowDuration: Duration): String = {
    s"$metric.${windowDuration.length}${windowDuration.unit}"
  }

  def serializeHistogram(histogram: Histogram) : ByteBuffer = {
    val buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = histogram.encodeIntoCompressedByteBuffer(buffer)

    buffer.limit(bytesEncoded)
    buffer.rewind()

    buffer
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = {
    Histogram.decodeFromCompressedByteBuffer(bytes, 0)
  }

}