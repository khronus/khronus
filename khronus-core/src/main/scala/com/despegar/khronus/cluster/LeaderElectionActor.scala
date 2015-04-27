package com.despegar.khronus.cluster

import akka.actor.{Props, Actor}
import scala.concurrent.duration._

class LeaderElection extends Actor {
  import context.dispatcher

  val tick =
    context.system.scheduler.schedule(500 millis, 1000 millis, self, "tick")

  override def postStop() = tick.cancel()

  def receive = {
    case "tick" => {
      println("<-------- LEADER ELECTION!")
    }
  }
}

object LeaderElection {
  def props: Props = Props(classOf[LeaderElection])
}
