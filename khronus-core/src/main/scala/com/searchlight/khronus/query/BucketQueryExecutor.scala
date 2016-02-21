package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import com.searchlight.khronus.store.BucketSupport

import scala.concurrent.Future
import scala.concurrent.duration._

trait BucketServiceSupport {
  def bucketService: BucketService = BucketService.instance
}

trait BucketService {
  def retrieve(subMetric: SubMetric, range: TimeRange, resolution: Option[Duration]): Future[BucketSlice[Bucket]]
}

object BucketService {
  val instance = new CassandraBucketService
}

class CassandraBucketService extends BucketService with BucketSupport {
  override def retrieve(subMetric: SubMetric, range: TimeRange, resolution: Option[Duration]): Future[BucketSlice[Bucket]] = {
    //TODO: select the resolution that better fits the given time range
    val metric = subMetric.asMetric()
    getStore(metric.mtype).slice(metric, range.from, range.to, resolution.getOrElse(1 minute))
  }
}
