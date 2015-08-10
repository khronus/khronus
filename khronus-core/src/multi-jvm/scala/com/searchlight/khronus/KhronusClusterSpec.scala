
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



package com.searchlight.khronus

import akka.actor.{ActorRef, Actor, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.routing.{ClusterRouterPoolSettings, ClusterRouterPool}
import akka.contrib.pattern.ClusterSingletonManager
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.routing.RoundRobinPool
import akka.testkit.ImplicitSender
import com.searchlight.khronus.cluster.{RouterProvider, Master}
import com.searchlight.khronus.model.Metric
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.duration._

class KhronusClusterSpecMultiJvmNode1 extends KhronusClusterSpec

class KhronusClusterSpecMultiJvmNode2 extends KhronusClusterSpec

class KhronusClusterSpecMultiJvmNode3 extends KhronusClusterSpec

class KhronusClusterSpecMultiJvmNode4 extends KhronusClusterSpec


class KhronusClusterSpec extends MultiNodeSpec(KhronusClusterSpecConfig) with STMultiNodeSpec with ImplicitSender {

  import KhronusClusterSpecConfig._

  def initialParticipants = roles.size

  val seedAddress = node(seed).address
  val masterAddress = node(master).address
  val worker1Address = node(worker1).address
  val worker2Address = node(worker2).address

  muteDeadLetters(classOf[Any])(system)

  "A Khronus cluster" should {
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

class TestMaster extends Master with LocalRouterProvider {
  import scala.concurrent.Future

  override def lookupMetrics: Future[Seq[Metric]] = Future.successful(Seq(Metric("a", "histogram")))
}

trait LocalRouterProvider extends RouterProvider {
  this: Actor ⇒

  override def createRouter: ActorRef = {
    context.actorOf(ClusterRouterPool(RoundRobinPool(10), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 20,
      allowLocalRoutees = true, useRole = None)).props(com.searchlight.khronus.cluster.Worker.props), "worker-router")
  }
}

object KhronusClusterSpecConfig extends MultiNodeConfig {

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
      | khronus {
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
