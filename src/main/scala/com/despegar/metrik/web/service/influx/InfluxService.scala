package com.despegar.metrik.web.service.influx

import spray.routing.HttpService
import akka.actor.Actor
import spray.http.MediaTypes._
import com.despegar.metrik.store.MetaSupport
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.despegar.metrik.util.Logging

/**
 * Created by aholman on 21/10/14.
 */
class InfluxActor extends Actor with InfluxService {

  def actorRefFactory = context

  def receive = runRoute(influxRoute)

}

case class QueryString(queryString: String)

trait InfluxService extends HttpService with MetaSupport with Logging {
  val ListSeries = "list series"

  val influxRoute = path("metrik" / "influx") {
    parameters('q) { queryString ⇒
      get {
        log.info(s"GET /metrik/influx - Query: [$queryString]")
        respondWithMediaType(`application/json`) {
          complete {
            import com.despegar.metrik.web.service.influx.InfluxSeriesProtocol._
            resolveInfluxQuery(queryString)
          }
        }
      }
    }
  }

  def resolveInfluxQuery(queryString: String): Future[Seq[InfluxSeries]] = {
    if (ListSeries.equalsIgnoreCase(queryString)) {
      log.info("Starting Influx list series")
      metaStore.retrieveMetrics map { results ⇒ results.map { x ⇒ new InfluxSeries(x) } }
    } else {
      log.error(s"Influx query [$queryString] is not supported")
      throw new UnsupportedOperationException(s"Influx query [$queryString] is not supported")
    }

  }
}

