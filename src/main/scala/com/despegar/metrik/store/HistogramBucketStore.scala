package com.despegar.metrik.store

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
    val key = s"$metric.$windowDuration"

    val result = Cassandra.keyspace.prepareQuery(columnFamily).getKey(key).withColumnRange(1, System.currentTimeMillis(), false, LIMIT).execute()

    result.getResult().asScala.map { column =>
      val timestamp = column.getName()
      val histogram = deserializeHistogram(column.getByteArrayValue())
      HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
    }.toSeq
    
  }

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {

  }

  private def deserializeHistogram(bytes: Array[Byte]): Histogram = null

}