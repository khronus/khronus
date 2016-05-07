package com.searchlight.khronus.service

import akka.actor.Props
import com.searchlight.khronus.api.MetricBatch
import com.searchlight.khronus.store.MetricMeasurementStoreSupport
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ ConcurrencySupport, JacksonJsonSupport }
import spray.http.StatusCodes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Failure
import scala.util.control.NonFatal

class APIActor extends HttpServiceActor with APIService with KhronusHandlerException {
  def receive = runRoute(metricsRoute)
}

object APIActor {
  val Name = "khronus-actor"
  val Path = "khronus/metrics"

  def props = Props[APIActor]
}

trait APIService extends HttpService with MetricMeasurementStoreSupport with JacksonJsonSupport with Logging with ConcurrencySupport {

  override def loggerName = classOf[APIService].getName

  implicit val executionContext: ExecutionContext = executionContext("metric-receiver-endpoint")

  val metricsRoute: Route =
    decompressRequest(Gzip, NoEncoding) {
      post {
        entity(as[MetricBatch]) { metricBatch ⇒
          complete {
            Future {
              metricStore.storeMetricMeasurements(metricBatch.metrics)
            }
            OK
          }
        }
      }
    } ~ get {
      parameters('q, 'from ?, 'to ?, 'res ?)
        .as(ApiRequest) { apiRequest ⇒
          complete {
            OK
          }
        }

    }

}

case class ApiRequest(q: String, from: Option[String], to: Option[String], res: Option[String])

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