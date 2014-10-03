package com.despegar.metrik.cluster

import akka.actor.{Actor, ActorLogging}

class Worker extends Actor with ActorLogging {

  def receive = {
    case MasterWorkerProtocol.Work(series) => {
      println(s"----------------------------------------------------------working with:$series")
      Thread.sleep(2000)

      sender() ! MasterWorkerProtocol.Finish(series)
    }
  }
}
