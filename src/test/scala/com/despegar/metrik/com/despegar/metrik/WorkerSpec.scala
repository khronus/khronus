package com.despegar.metrik.com.despegar.metrik

import akka.actor.ActorSystem
import akka.testkit.{ TestKitBase, TestActorRef, ImplicitSender, TestKit }
import com.despegar.metrik.cluster._
import com.typesafe.config.ConfigFactory
import org.scalatest.{ WordSpecLike, BeforeAndAfterAll, Matchers }

class WorkerSpec extends TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {

  implicit lazy val system: ActorSystem = ActorSystem("Worker-Spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |}
    """.stripMargin))

  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  val worker = TestActorRef[Worker]

  "The Worker actor" should {
    "ignore the Work message if it's received before register in the cluster" in {
      worker ! Work("some work")
      expectNoMsg()
    }

    "respond with a Register when receiving a DiscoverWorkers  message" in {
      worker ! Heartbeat
      expectMsgClass(classOf[Register])
    }

    "respond with a WorkDone when receiving a Work message and finalize it successful" in {
      worker ! Work("some work")
      expectMsgClass(classOf[WorkDone])
    }
  }
}
