package com.despegar.metrik.util

import akka.actor.{ ActorRef, ActorSystem }

trait ActorSystemSupport {
  implicit def system = ActorSystemSupport.system
}

object ActorSystemSupport {
  val system = ActorSystem("metrik-system")
}

case class Register(path: String, actor: ActorRef)
case class MetrikStarted(handler: ActorRef)