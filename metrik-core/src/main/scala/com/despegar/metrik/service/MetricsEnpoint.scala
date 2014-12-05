package com.despegar.metrik.service

import akka.actor.Props
import com.despegar.metrik.model.MetricBatch
import com.despegar.metrik.model.MetricBatchProtocol._
import com.despegar.metrik.store.MetricMeasurementStoreSupport
import com.despegar.metrik.util.log.Logging
import spray.http.StatusCodes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing.{ HttpService, HttpServiceActor, Route }

class MetrikActor extends HttpServiceActor with MetricsEnpoint with MetrikHandlerException {
  def receive = runRoute(metricsRoute)
}

object MetrikActor {
  val Name = "metrik-actor"
  val Path = "metrik/metrics"

  def props = Props[MetrikActor]
}

trait MetricsEnpoint extends HttpService with MetricMeasurementStoreSupport with Logging {

  override def loggerName = classOf[MetricsEnpoint].getName()

  val metricsRoute: Route =
    decompressRequest(Gzip, NoEncoding) {
      post {
        entity(as[MetricBatch]) { metricBatch ⇒
          complete {
            metricStore.storeMetricMeasurements(metricBatch.metrics)
            OK
          }
        }
      }
    }

}