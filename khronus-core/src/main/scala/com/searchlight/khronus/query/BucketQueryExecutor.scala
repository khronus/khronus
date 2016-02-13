package com.searchlight.khronus.query

import com.searchlight.khronus.model._
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
    getStore(metric.mtype).slice(metric, range.from, range.to, resolution)
  }
}
