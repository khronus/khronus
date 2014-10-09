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

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.routing.RoundRobinPool
import akka.testkit._
import com.despegar.metrik.cluster.Master.Initialize
import com.despegar.metrik.cluster._
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class MasterSpec extends TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {

  implicit lazy val system: ActorSystem = ActorSystem("Master-Spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |}
      |
      |metrik {
      |  master {
      |    tick-expression = "0/1 * * * * ?"
      |    discovery-start-delay = 1 second
      |    discovery-interval = 5 seconds
      |  }
      |}
    """.stripMargin))

  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  "The Master actor" should {
    "receive a Tick message" in new MasterFixture {
      //master.rereceive(Initialize)
      probe.expectMsg(Heartbeat)
      //      expectMsgClass(classOf[WorkDone])
    }
  }
  class Wrapper(target: ActorRef) extends Actor {
    def receive = {
      case x ⇒ target forward x
    }
  }

  trait MasterFixture {
    val probe = TestProbe()
    class TestMaster extends Master with LocalRouterProvider

    trait LocalRouterProvider extends RouterProvider {
      this: Actor ⇒

      override def createRouter: ActorRef = {
        context.actorOf(RoundRobinPool(1).props(Props(new Wrapper(probe.ref))), "worker-router")
      }
    }
    val master = TestActorRef(Props(new TestMaster()))
  }
}

