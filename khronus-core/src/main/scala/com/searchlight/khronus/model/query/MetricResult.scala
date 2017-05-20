package com.searchlight.khronus.model.query

import com.searchlight.khronus.model.{ Bucket, BucketSlice, Metric }

import scala.concurrent.Future

case class MetricResult(metric: Metric, slice: Future[BucketSlice[Bucket]])
