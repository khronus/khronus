package com.searchlight.khronus.controller

import akka.actor.Props
import com.searchlight.khronus.actor.KhronusHandlerException
import com.searchlight.khronus.api.sql.jsql.JSQLParser
import com.searchlight.khronus.service.QueryService
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ CORSSupport, ConcurrencySupport, JacksonJsonSupport }
import spray.http.MediaTypes._
import spray.httpx.encoding.{ Gzip, NoEncoding }
import spray.routing._

import scala.concurrent.ExecutionContext

class QueryEndpoint extends HttpServiceActor with JacksonJsonSupport with CORSSupport with Logging with KhronusHandlerException with ConcurrencySupport {

  private val parser = new JSQLParser
  private val queryService = QueryService()
  implicit val ex: ExecutionContext = executionContext("query-endpoint-worker")

  private val route: Route =
    compressResponse(NoEncoding, Gzip) {
      respondWithCORS {
        get {
          parameters('sql) { (sql) ⇒
            respondWithMediaType(`application/json`) {
              complete {
                log.info(s"Query: $sql")
                queryService.executeQuery(parser.parse(sql))
              }
            }
          }
        }
      }
    }

  def receive = runRoute(route)

}

object QueryEndpoint {
  val Name = "query-endpoint"

  val Path = "v1/query"

  def props = Props[QueryEndpoint]
}
