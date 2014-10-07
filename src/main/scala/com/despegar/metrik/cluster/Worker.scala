package com.despegar.metrik.cluster

import akka.actor.{Actor, ActorLogging}

class Worker extends Actor with ActorLogging {
  import context._

  def receive:Receive = idle

  def idle:Receive = {
    case DiscoverWorkers =>
      sender ! Register(self)
      become(ready)
      log.info("Worker ready to work: [{}]", self.path)
  }

  def ready:Receive = {
    case Work(metric) =>
      log.info("Starting processing Metric: [{}]", metric)
      Thread.sleep(10000)
      sender() ! WorkDone(self)

    case everythingElse => //ignore
  }
}
