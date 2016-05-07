package com.searchlight.khronus.query

import com.searchlight.khronus.model._
import com.searchlight.khronus.store.BucketSupport
import com.searchlight.khronus.util.Settings

import scala.concurrent.Future
import scala.concurrent.duration._

trait BucketServiceSupport {
  def bucketService: BucketService = BucketService.instance
}

trait BucketService {
  def retrieve(metric: Metric, slice: Slice, resolution: Option[Duration]): Future[BucketSlice[Bucket]]
}

object BucketService {
  val instance = new CassandraBucketService
}

class CassandraBucketService extends BucketService with BucketSupport {
  private val maxResolution = Settings.Dashboard.MaxResolutionPoints
  private val minResolution = Settings.Dashboard.MinResolutionPoints
  private val forceResolution = true

  override def retrieve(metric: Metric, slice: Slice, resolution: Option[Duration]): Future[BucketSlice[Bucket]] = {
    val adjustedResolution = slice.getAdjustedResolution(resolution.getOrElse(1 minute), forceResolution, minResolution, maxResolution)
    getStore(metric.mtype).slice(metric, slice.from, slice.to, adjustedResolution)
  }
}
