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

