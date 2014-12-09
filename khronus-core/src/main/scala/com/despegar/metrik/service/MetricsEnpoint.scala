package com.despegar.metrik.service

import akka.actor.Props
import com.despegar.metrik.model.MetricBatch
import com.despegar.metrik.model.MetricBatchProtocol._
import com.despegar.metrik.store.MetricMeasurementStoreSupport
import com.despegar.metrik.util.log.Logging
import spray.http.StatusCodes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing._

import scala.util.Failure
import scala.util.control.NonFatal

class MetrikActor extends HttpServiceActor with MetricsEnpoint with MetrikHandlerException {
  def receive = runRoute(metricsRoute)
}

object MetrikActor {
  val Name = "metrik-actor"
  val Path = "metrik/metrics"

  def props = Props[MetrikActor]
}

trait MetricsEnpoint extends HttpService with MetricMeasurementStoreSupport with Logging {

  import com.despegar.metrik.service.SprayMetrics._

  override def loggerName = classOf[MetricsEnpoint].getName()

  val metricsRoute: Route =
    decompressRequest(Gzip, NoEncoding) {
      post {
        time("sprayTimeWithDeserialization") {
          entity(as[MetricBatch]) { metricBatch ⇒
            time("sprayTimeWithoutDeserialization") {
              complete {
                val start = System.currentTimeMillis()
                metricStore.storeMetricMeasurements(metricBatch.metrics)
                log.info(s"storeMetricMeasurements time spent ${System.currentTimeMillis() - start} ms")
                OK
              }
            }
          }
        }
      }

    }

}

object SprayMetrics extends Logging {

  import spray.routing.directives.BasicDirectives._

  def around(before: RequestContext ⇒ (RequestContext, Any ⇒ Any)): Directive0 =
    mapInnerRoute { inner ⇒
      ctx ⇒
        val (ctxForInnerRoute, after) = before(ctx)
        try inner(ctxForInnerRoute.withRouteResponseMapped(after))
        catch {
          case NonFatal(ex) ⇒ after(Failure(ex))
        }
    }

  def buildAfter(name: String, start: Long): Any ⇒ Any = { possibleRsp: Any ⇒
    possibleRsp match {
      case _ ⇒
        log.info(s"$name time spent ${System.currentTimeMillis() - start} ms")
    }
    possibleRsp
  }

  def time(name: String): Directive0 =
    around { ctx ⇒
      val timerContext = System.currentTimeMillis()
      (ctx, buildAfter(name, timerContext))
    }

}