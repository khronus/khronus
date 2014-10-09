/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.com.despegar.metrik

import akka.actor.{ActorRef, Actor, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.routing.{ClusterRouterPoolSettings, ClusterRouterPool}
import akka.contrib.pattern.ClusterSingletonManager
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.routing.RoundRobinPool
import akka.testkit.ImplicitSender
import com.despegar.metrik.cluster.{RouterProvider, Master}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.duration._

class MetrikClusterSpecMultiJvmNode1 extends MetrikClusterSpec

class MetrikClusterSpecMultiJvmNode2 extends MetrikClusterSpec

class MetrikClusterSpecMultiJvmNode3 extends MetrikClusterSpec

class MetrikClusterSpecMultiJvmNode4 extends MetrikClusterSpec


class MetrikClusterSpec extends MultiNodeSpec(MetrikClusterSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import MetrikClusterSpecConfig._

  def initialParticipants = roles.size

  val seedAddress = node(seed).address
  val masterAddress = node(master).address
  val worker1Address = node(worker1).address
  val worker2Address = node(worker2).address

  muteDeadLetters(classOf[Any])(system)

  "A Metrik cluster" should {
    "form the cluster" in within(10 seconds) {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])

      Cluster(system).join(seedAddress)

      receiveN(4).map {
        case MemberUp(m) => m.address
      }.toSet must be(Set(seedAddress, masterAddress, worker1Address, worker2Address))

      Cluster(system).unsubscribe(testActor)

      enterBarrier("cluster-up")
    }

    "process a metric once the cluster is running" in within(30 seconds) {
      runOn(seed) {
        system.actorOf(ClusterSingletonManager.props(Props(new TestMaster), "master", PoisonPill, Some("master")), "singleton-manager")
      }
      enterBarrier("work-done")
    }
  }
}

class TestMaster extends Master with LocalRouterProvider

trait LocalRouterProvider extends RouterProvider {
  this: Actor ⇒

  override def createRouter: ActorRef = {
    context.actorOf(ClusterRouterPool(RoundRobinPool(10), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 20,
      allowLocalRoutees = true, useRole = None)).props(com.despegar.metrik.cluster.Worker.props), "worker-router")
  }
}

object MetrikClusterSpecConfig extends MultiNodeConfig {

  val seed = role("seed")
  val master = role("master")
  val worker1 = role("worker-1")
  val worker2 = role("worker-2")

  commonConfig(ConfigFactory.parseString(
    """
      | akka.actor.provider="akka.cluster.ClusterActorRefProvider"
      | akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.loglevel = INFO
      | akka.remote.log-remote-lifecycle-events = off
      | akka.log-dead-letters = off
      | akka.cluster.auto-down-unreachable-after = 0s
      |
      | metrik {
      |   master {
      |     tick-expression = "0/1 * * * * ?"
      |     discovery-start-delay = 1 second
      |     discovery-interval = 5 seconds
      |   }
      |}
    """.stripMargin))

  nodeConfig(seed, master)(ConfigFactory.parseString("akka.cluster.roles =[master]"))
}

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}