package com.despegar.metrik.store

import java.nio.ByteBuffer
import java.util.concurrent.Executors
import com.despegar.metrik.model.HistogramBucket
import scala.collection.JavaConverters._
import org.HdrHistogram.Histogram
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.LongSerializer
import scala.concurrent._
import scala.concurrent.duration._
import com.netflix.astyanax.ColumnListMutation
import com.despegar.metrik.util.Logging
import com.netflix.astyanax.model.Column

trait HistogramBucketStore {

  def sliceUntilNow(metric: String, windowDuration: Duration): Future[Seq[HistogramBucket]]

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])

  def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])

}

trait HistogramBucketSupport {
  def histogramBucketStore: HistogramBucketStore = CassandraHistogramBucketStore
}

object CassandraHistogramBucketStore extends HistogramBucketStore with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(1 millis, 30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour) //FIXME put configured windows
  val columnFamilies = windowDurations.map(duration => (duration, ColumnFamily.newColumnFamily(getColumnFamilyName(duration), StringSerializer.get(), LongSerializer.get()))).toMap

  val LIMIT = 1000

  implicit val asyncExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  def initialize = columnFamilies.foreach(cf => Cassandra.createColumnFamily(cf._2))
  
  def sliceUntilNow(metric: String, windowDuration: Duration): Future[Seq[HistogramBucket]] = {
    Future {
      executeSlice(metric, windowDuration)
    } map { _.map { toHistogramBucketOf(windowDuration) _ }.toSeq }
  }

  private def executeSlice(metric: String, windowDuration: Duration) = {
    Cassandra.keyspace.prepareQuery(columnFamilies(windowDuration)).getKey(getKey(metric, windowDuration))
      .withColumnRange(infinite, now, false, LIMIT).execute().getResult().asScala
  }

  private def toHistogramBucketOf(windowDuration: Duration)(column: Column[java.lang.Long]) = {
    val timestamp = column.getName()
    val histogram = deserializeHistogram(column.getByteBufferValue)
    HistogramBucket(timestamp / windowDuration.toMillis, windowDuration, histogram)
  }

  def store(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    log.debug(s"Storing ${histogramBuckets.length} histogram buckets for metric $metric in window $windowDuration")
    mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) =>
      mutation.putColumn(bucket.timestamp, serializeHistogram(bucket.histogram))
    }
  }

  def remove(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket]) = {
    log.debug(s"Removing ${histogramBuckets.length} histogram buckets for metric $metric in window $windowDuration")
    mutate(metric, windowDuration, histogramBuckets) { (mutation, bucket) =>
      mutation.deleteColumn(bucket.timestamp)
    }
  }

  private def mutate(metric: String, windowDuration: Duration, histogramBuckets: Seq[HistogramBucket])(f: (ColumnListMutation[java.lang.Long], HistogramBucket) => Unit) = {
    Future {
      val mutationBatch = Cassandra.keyspace.prepareMutationBatch()
      val mutation = mutationBatch.withRow(columnFamilies(windowDuration), getKey(metric, windowDuration))
      histogramBuckets.foreach(f(mutation, _))
      mutationBatch.execute
      log.debug("Mutation successful")
    } onFailure {
      case e: Exception => log.error("Mutation failed", e)
    }
  }

  private def getColumnFamilyName(duration: Duration) = s"bucket${duration.length}${duration.unit}"

  private def getKey(metric: String, windowDuration: Duration): String = metric

  private def serializeHistogram(histogram: Histogram): ByteBuffer = {
    val buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes)
    val bytesEncoded = histogram.encodeIntoCompressedByteBuffer(buffer) //TODO: Find a better way to do this serialization
    log.debug(s"Histogram with ${histogram.getTotalCount()} measures encoded and compressed into $bytesEncoded bytes")
    buffer.limit(bytesEncoded)
    buffer.rewind()
    buffer
  }

  private def deserializeHistogram(bytes: ByteBuffer): Histogram = Histogram.decodeFromCompressedByteBuffer(bytes, 0)

  private def now = System.currentTimeMillis()

  private def infinite = 1L

}