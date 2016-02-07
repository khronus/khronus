package com.searchlight.khronus.query

import com.searchlight.khronus.model.{ Bucket, BucketSlice, SubMetric }
import com.searchlight.khronus.store.BucketSupport

import scala.concurrent.Future
import scala.concurrent.duration._

trait BucketQuerySupport {
  def bucketQueryExecutor: BucketQueryExecutor = BucketQueryExecutor
}

trait BucketQueryExecutor {
  def retrieve(subMetric: SubMetric, range: TimeRange): Future[BucketSlice[Bucket]]
}

object BucketQueryExecutor extends BucketQueryExecutor with BucketSupport {
  override def retrieve(subMetric: SubMetric, range: TimeRange): Future[BucketSlice[Bucket]] = {
    //TODO: select the resolution that better fits the given time range
    val resolution = 1 minute
    val metric = subMetric.asMetric()
    //TODO: refactor me
    metric.mtype match {
      case "counter"   ⇒ counterBucketStore.slice(metric, range.from, range.to, resolution)
      case "histogram" ⇒ histogramBucketStore.slice(metric, range.from, range.to, resolution)
    }
  }
}
