
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

import akka.actor.{ Props, ActorSystem }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestKitBase }
import com.despegar.metrik.cluster._
import com.despegar.metrik.model.TimeWindowChain
import com.typesafe.config.ConfigFactory
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.despegar.metrik.model.Metric

class WorkerSpec extends TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll with MockitoSugar {

  implicit lazy val system: ActorSystem = ActorSystem("Worker-Spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |}
    """.stripMargin))

  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  trait TimeWindowChainProviderMock extends TimeWindowChainProvider {
    override val timeWindowChain = mock[TimeWindowChain]
    when(timeWindowChain.process(any())).thenReturn(Future(Seq(Thread.sleep(1000))))
  }

  val worker = TestActorRef(Props(new Worker with TimeWindowChainProviderMock))

  "The Worker actor" should {
    "ignore the Work message if it's received before register in the cluster" in {
      worker ! Work(Metric("some work", "histogram"))
      expectNoMsg()
    }

    "respond with a Register when receiving a DiscoverWorkers  message" in {
      worker ! Heartbeat
      expectMsgClass(classOf[Register])
    }

    "respond with a WorkDone when receiving a Work message and finalize it successful" in {
      worker ! Work(Metric("some work", "histogram"))
      expectMsgClass(classOf[WorkDone])
    }
  }
}
