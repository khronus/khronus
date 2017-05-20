package com.searchlight.khronus.controller

import akka.actor.Props
import com.searchlight.khronus.actor.KhronusHandlerException
import com.searchlight.khronus.model.MetricBatch
import com.searchlight.khronus.service.{ MetaService, IngestionService }
import com.searchlight.khronus.util.{ JacksonJsonSupport, ConcurrencySupport }
import spray.http.StatusCodes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing._

case class MetaRequest(name: Option[String], `type`: Option[String])

class MetricsEndpoint extends HttpServiceActor with KhronusHandlerException with ConcurrencySupport with JacksonJsonSupport {

  override def loggerName = classOf[MetricsEndpoint].getName

  private val ingestionService = IngestionService()
  private val metaService = MetaService()

  val metricsRoute: Route =
    decompressRequest(Gzip, NoEncoding) {
      post {
        entity(as[MetricBatch]) {
          metricBatch ⇒
            complete {
              ingestionService.store(metricBatch.metrics)
              OK
            }
        }
      }
    } ~ get {
      parameters('name ?, 'type ?)
        .as(MetaRequest) {
          metaRequest ⇒
            complete {
              metaService.getMetrics(metaRequest)
            }
        }

    }

  def receive = runRoute(metricsRoute)
}

object MetricsEndpoint {
  val Name = "metrics-endpoint"
  val Path = "v1/metrics"

  def props = Props[MetricsEndpoint]
}