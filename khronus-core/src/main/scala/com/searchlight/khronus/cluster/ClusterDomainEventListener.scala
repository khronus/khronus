/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.searchlight.khronus.cluster

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.ClusterEvent._
import akka.cluster.{ Cluster, MemberStatus }

class ClusterDomainEventListener extends Actor with ActorLogging {

  def receive = {
    case state: CurrentClusterState ⇒ log.info(s"Current state of the cluster: $state")
    case MemberUp(member)           ⇒ log.info(s"$member UP.")
    case MemberExited(member)       ⇒ log.info(s"$member EXITED.")
    case UnreachableMember(member)  ⇒ log.info(s"$member UNREACHABLE")
    case ReachableMember(member)    ⇒ log.info(s"$member REACHABLE")
    case MemberRemoved(member, previousState) ⇒
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
  def props: Props = Props(classOf[ClusterDomainEventListener])
}