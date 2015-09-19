/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

import akka.actor.{ ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestKitBase }
import com.searchlight.khronus.model.{ SystemClock, Clock, Metric, TimeWindowChain }
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike }
import scala.concurrent.Future
import com.typesafe.config.ConfigFactory

class WorkerSpec extends TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll with BeforeAndAfter with MockitoSugar {

  implicit lazy val system: ActorSystem = ActorSystem("Worker-Spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |  actor {
      |    provider = "akka.actor.LocalActorRefProvider"
      |  }
      |}
    """.stripMargin))

  val metric = Metric("some work", "histogram")

  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  val worker = TestActorRef(Props(new Worker {
    override def timeWindowChain = new TimeWindowChain {
      override def process(metrics: Seq[Metric])(implicit clock: Clock = SystemClock) = Future.successful(Unit)
    }
  }))

  "The Worker actor" should {
    "ignore the Work message if it's received before register in the cluster" in {
      worker ! Work(Seq(metric))
      expectNoMsg()
    }

    "respond with a Register when receiving a DiscoverWorkers  message" in {
      worker ! Heartbeat
      expectMsgClass(classOf[Register])
    }

    "respond with a WorkDone when receiving a Work message and finalize it successful" in {
      worker ! Work(Seq(metric))
      expectMsgClass(classOf[WorkDone])
    }
  }
}
