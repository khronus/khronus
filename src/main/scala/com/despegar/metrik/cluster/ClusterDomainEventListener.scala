package com.despegar.metrik.cluster

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, MemberStatus}

class ClusterDomainEventListener extends Actor with ActorLogging {

  def receive = {
    case state: CurrentClusterState =>  log.info(s"Current state of the cluster: $state")
    case MemberUp(member) => log.info(s"$member UP.")
    case MemberExited(member) => log.info(s"$member EXITED.")
    case UnreachableMember(member) => log.info(s"$member UNREACHABLE")
    case ReachableMember(member) => log.info(s"$member REACHABLE")
    case MemberRemoved(member, previousState) =>
      if (previousState == MemberStatus.Exiting) {
        log.info(s"Member $member Previously gracefully exited, REMOVED.")
      } else {
        log.info(s"$member Previously downed after unreachable, REMOVED.")
      }
  }


  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])
    super.preStart()
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
    super.postStop()
  }
}

object ClusterDomainEventListener {
  def props:Props = Props(classOf[ClusterDomainEventListener])
}