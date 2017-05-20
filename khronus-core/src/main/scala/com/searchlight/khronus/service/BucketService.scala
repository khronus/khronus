package com.searchlight.khronus.service

import com.searchlight.khronus.dao.BucketSupport
import com.searchlight.khronus.model._
import com.searchlight.khronus.model.query.TimeRange

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait BucketService {
  def retrieve(metric: Metric, timeRange: TimeRange, resolution: Duration): Future[BucketSlice[Bucket]]

  def save(metric: Metric, buckets: Seq[Bucket], resolution: Duration = 1 milliseconds): Future[Unit]
}

case class CassandraBucketService() extends BucketService with BucketSupport {

  private def ranges(timeRange: TimeRange, resolution: Duration): Seq[(TimeRange, Duration)] = {
    Seq((timeRange, 1 minute))
  }

  override def retrieve(metric: Metric, timeRange: TimeRange, resolution: Duration): Future[BucketSlice[Bucket]] = {
    Future.sequence(ranges(timeRange, resolution).map {
      case (range, rangeResolution) ⇒
        getStore(metric.mtype).slice(metric, range.from, range.to, rangeResolution)
    }).map { slices ⇒
      BucketSlice(slices.flatMap(_.results))
    }
  }

  override def save(metric: Metric, buckets: Seq[Bucket], resolution: Duration = 1 milliseconds): Future[Unit] = {
    getStore(metric.mtype).store(metric, resolution, buckets)
  }
}