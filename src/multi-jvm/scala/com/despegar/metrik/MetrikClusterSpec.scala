package com.despegar.metrik.com.despegar.metrik

import akka.actor.{Props, Actor, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig, MultiNodeSpec}
import akka.routing.RoundRobinPool
import akka.testkit.ImplicitSender
import com.despegar.metrik.cluster.{Master, RouterProvider, WorkDone}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.duration._

class WordsClusterSpecMultiJvmNode1 extends MetrikClusterSpec
class WordsClusterSpecMultiJvmNode2 extends MetrikClusterSpec
class WordsClusterSpecMultiJvmNode3 extends MetrikClusterSpec
class WordsClusterSpecMultiJvmNode4 extends MetrikClusterSpec

class MetrikClusterSpec extends MultiNodeSpec(WordsClusterSpecConfig) with STMultiNodeSpec with ImplicitSender{
  import WordsClusterSpecConfig._

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

    "process a metric once the cluster is running" in  within(3 seconds) {
        runOn(master) {
          system.actorOf(Props(new TestMaster), "Master")
          expectMsg(WorkDone)
        }
        enterBarrier("work-done")
      }
    }
  }

  class TestMaster extends Master with LocalRouterProvider

  trait LocalRouterProvider extends RouterProvider {
    this: Actor ⇒

    override def createRouter: ActorRef = {
      context.actorOf(RoundRobinPool(1).props(com.despegar.metrik.cluster.Worker.props), "worker-router")
    }
}

object WordsClusterSpecConfig extends MultiNodeConfig {

  val seed = role("seed")
  val master = role("master")
  val worker1 = role("worker-1")
  val worker2 = role("worker-2")

  commonConfig(ConfigFactory.parseString(
    """
      | akka.actor.provider="akka.cluster.ClusterActorRefProvider"
      | akka.cluster.auto-join = off
      | akka.cluster.auto-down = on
      | akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.loglevel = INFO
      | akka.remote.log-remote-lifecycle-events = off
      | akka.log-dead-letters = off
      |
      | metrik {
      |   master {
      |     tick-expression = "0/1 * * * * ?"
      |     discovery-start-delay = 1 second
      |     discovery-interval = 5 seconds
      |   }
      |}
    """.stripMargin))
}

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()
}