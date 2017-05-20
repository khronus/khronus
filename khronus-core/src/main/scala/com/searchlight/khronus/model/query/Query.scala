package com.searchlight.khronus.model.query

import scala.concurrent.duration.Duration

case class Query(projections: Seq[Projection], selectors: Seq[Selector], timeRange: Option[TimeRange], filter: Option[Filter],
  resolution: Option[Duration] = None, group: Map[Selector, Seq[String]] = Map(),
  limit: Int = Int.MaxValue, orderAsc: Boolean = true, fillValue: Option[Double] = None)