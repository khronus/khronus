package com.despegar.metrik.util

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import scala.concurrent.duration._
import com.despegar.metrik.web.service.HandlerActor

object Metrik extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("metrik-system")

  // create and start our service actor
  val handler = system.actorOf(Props[HandlerActor], "handler-actor")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(handler, interface = "localhost", port = 8080)
}
