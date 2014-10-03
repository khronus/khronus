package com.despegar.metrik.util

import akka.actor._
import akka.contrib.pattern.ClusterSingletonManager
import com.despegar.metrik.cluster.{Master, ClusterDomainEventListener}

object Boot extends App {

  val system = ActorSystem("metrik-system")

  system.actorOf(ClusterDomainEventListener.props, "cluster-listener")

  system.actorOf(ClusterSingletonManager.props(
    singletonProps = Master.props,
    singletonName = "master",
    terminationMessage = PoisonPill,
    role = Some("master")), "singleton-manager")
}

