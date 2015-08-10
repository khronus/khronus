/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.searchlight.khronus.service

import akka.actor._
import com.searchlight.khronus.service.HandShakeProtocol.Register
import com.searchlight.khronus.util.CORSSupport
import spray.http.{ Timedout, HttpRequest, HttpResponse, StatusCodes }
import spray.http.StatusCodes._
import spray.httpx.marshalling.ToResponseMarshaller
import spray.routing._
import spray.util.LoggingContext

class KhronusHandler extends HttpServiceActor with ActorLogging with KhronusHandlerException {
  var composedRoute: Route = reject

  def createEndpointRoute(path: String, actor: ActorRef): Route =
    pathPrefix(separateOnSlashes(path)) {
      requestContext ⇒ actor ! requestContext
    }

  val registerReceive: Receive = {
    case Register(path, actor) ⇒
      log.info(s"Registering endpoint: $path")
      composedRoute = composedRoute ~ createEndpointRoute(path, actor)
      context become receive
  }

  def receive = registerReceive orElse handleTimeouts orElse runRoute(composedRoute)

  def handleTimeouts: Receive = {
    case Timedout(request: HttpRequest) ⇒ {
      val message = s"Request timeout! URI: ${request.uri}"
      log.error(message)
      sender ! HttpResponse(StatusCodes.InternalServerError, message)
    }
  }

}

object KhronusHandler {
  val Name = "handler-actor"
  def props: Props = Props[KhronusHandler]
}

object HandShakeProtocol {
  case class Register(path: String, actor: ActorRef)
  case class KhronusStarted(handler: ActorRef)
}

trait KhronusHandlerException {
  implicit def khronusExceptionHandler(implicit settings: RoutingSettings, log: LoggingContext): ExceptionHandler =
    ExceptionHandler.apply {
      case e: UnsupportedOperationException ⇒ ctx ⇒ {
        log.error(s"Handling UnsupportedOperationException ${e.getMessage}", e)
        responseWithCORSHeaders(ctx, (BadRequest, s"${e.getMessage}"))
      }
      case e: Exception ⇒ ctx ⇒ {
        log.error(s"Handling Exception ${e.getMessage}", e)
        responseWithCORSHeaders(ctx, InternalServerError)
      }
    }

  private def responseWithCORSHeaders[T](ctx: RequestContext, response: T)(implicit marshaller: ToResponseMarshaller[T]) = {
    ctx.withHttpResponseHeadersMapped(_ ⇒ CORSSupport.headers).complete(response)(marshaller)
  }
}
