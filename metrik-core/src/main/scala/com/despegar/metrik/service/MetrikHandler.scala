/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.service

import akka.actor.{ ActorRef, Props }
import com.despegar.metrik.service.HandShakeProtocol.Register
import com.despegar.metrik.util.{ CORSSupport, Logging }
import spray.http.StatusCodes._
import spray.httpx.marshalling.ToResponseMarshaller
import spray.routing._

import scala.collection.concurrent.TrieMap

class MetrikHandler extends HttpServiceActor with MetrikExceptionHandler {
  val endpoints = TrieMap[String, ActorRef]()

  def createEndpointRoute(path: String, target: ActorRef): Route =
    pathPrefix(separateOnSlashes(path)) {
      ctx ⇒ target ! ctx
    }

  def composeRoute: Route = {
    endpoints.foldLeft[Route](reject) { (acc, next) ⇒
      val (path, actorRef) = next
      acc ~ createEndpointRoute(path, actorRef)
    }
  }

  val registerReceive: Receive = {
    case Register(path, actor) ⇒
      log.info(s"Registering endpoint: $path")
      endpoints.update(path, actor)
      context become receive
  }

  def receive = registerReceive orElse runRoute(composeRoute)
}

object MetrikHandler {
  val Name = "handler-actor"
  def props: Props = Props[MetrikHandler]
}

object HandShakeProtocol {
  case class Register(path: String, actor: ActorRef)
  case class MetrikStarted(handler: ActorRef)
}

trait MetrikExceptionHandler extends Logging {
  implicit def myExceptionHandler: ExceptionHandler =
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