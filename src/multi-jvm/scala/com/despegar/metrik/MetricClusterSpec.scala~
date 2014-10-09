package com.despegar.metrik.com.despegar.metrik

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberUp, CurrentClusterState}
import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import akka.remote.testkit.MultiNodeSpecCallbacks
import org.scalatest._
import org.scalatest.MustMatchers
import scala.concurrent.duration._

class WordsClusterSpecMultiJvmNode1 extends MetricClusterSpec
class WordsClusterSpecMultiJvmNode2 extends MetricClusterSpec
class WordsClusterSpecMultiJvmNode3 extends MetricClusterSpec
class WordsClusterSpecMultiJvmNode4 extends MetricClusterSpec

class MetricClusterSpec extends MultiNodeSpec(WordsClusterSpecConfig) with STMultiNodeSpec with ImplicitSender{
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
    """.stripMargin))
}
