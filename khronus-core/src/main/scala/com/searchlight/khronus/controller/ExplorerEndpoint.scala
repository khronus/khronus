package com.searchlight.khronus.controller

import akka.actor.Props
import com.searchlight.khronus.actor.KhronusHandlerException
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ CORSSupport, ConcurrencySupport, JacksonJsonSupport }
import spray.routing._

class ExplorerEndpoint extends HttpServiceActor with JacksonJsonSupport with CORSSupport with Logging with KhronusHandlerException with ConcurrencySupport {

  private val route: Route = getFromResource("explorer/main.html")

  def receive = runRoute(route)

}

object ExplorerEndpoint {
  val Name = "explorer-endpoint"

  val Path = "explorer"

  def props = Props[ExplorerEndpoint]
}

