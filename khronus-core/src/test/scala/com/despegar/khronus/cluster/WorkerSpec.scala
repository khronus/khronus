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

package com.despegar.khronus.cluster

import akka.actor.{ ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestKitBase }
import com.despegar.khronus.model.{ Metric, TimeWindowChain }
import com.despegar.khronus.util.BaseTest
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class WorkerSpec extends BaseTest with TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll with MockitoSugar {

  implicit lazy val system: ActorSystem = ActorSystem("Worker-Spec")

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
