package com.searchlight.khronus.service

import com.searchlight.khronus.controller.MetaRequest
import com.searchlight.khronus.dao.{ Meta, MetaStore, MetricMetadata }
import com.searchlight.khronus.util.log.Logging

case class MetaService(metaStore: MetaStore = Meta.metaStore) extends Logging {
  def getMetrics(metaRequest: MetaRequest): Seq[MetricMetadata] = {
    log.debug(s"Searching metrics matching $metaRequest...")
    val metrics = metaStore.searchMetrics(metaRequest.name.getOrElse(".*"))
    log.debug(s"Found ${metrics.size} metrics")
    metrics
  }
}
