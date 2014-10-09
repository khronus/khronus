package com.despegar.metrik.web.service

import akka.actor.Props
import akka.io.IO
import com.despegar.metrik.util.{ Settings, ActorSystemSupport }
import spray.can.Http

trait MetrikService {
  this: ActorSystemSupport ⇒

  val service = system.actorOf(Props[HandlerActor], "version-service")

  IO(Http) ! Http.Bind(service, Settings(system).Http.Interface, Settings(system).Http.Port)
}