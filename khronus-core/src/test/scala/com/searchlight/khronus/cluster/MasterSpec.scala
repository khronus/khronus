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

package com.searchlight.khronus.cluster

import akka.actor._
import akka.routing.{ ActorRefRoutee, Routees, RoundRobinGroup }
import akka.testkit._
import com.searchlight.khronus.cluster.Master.PendingMetrics
import com.searchlight.khronus.model.Metric
import com.typesafe.config.ConfigFactory
import org.scalatest.{ Ignore, BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MasterSpec extends TestKitBase with ImplicitSender
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {

  implicit lazy val system: ActorSystem = ActorSystem("Master-Spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = INFO
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |}
      |
      |khronus {
      |  master {
      |    tick-expression = "0/1 * * * * ?"
      |    discovery-start-delay = 1 second
      |    discovery-interval = 2 seconds
      |    worker-batch-size = 1
      |  }
      |}
    """.stripMargin))

  val timeout: FiniteDuration = 1500 millis
  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  "The Master actor" should {

    "send a Broadcast to heartbeat all workers every discovery-interval" in new ScheduledMasterProbeWorkerFixture {

      withDelay(1000) {
        // First heartbeat is after discovery-start-delay
        workerProbe1.expectMsg(timeout, Heartbeat)
        workerProbe2.expectMsg(timeout, Heartbeat)
      }

      for (_ ← 1 to 2) {
        withDelay(5000) {
          // Other hertbeats are after each discovery-interval
          workerProbe1.expectMsg(timeout, Heartbeat)
          workerProbe2.expectMsg(timeout, Heartbeat)
        }
      }

      master.stop()
    }

    "receive a Tick message at tick-expression" in new ScheduledMasterProbeWorkerFixture {
      master ! Register(worker1)
      master ! Register(worker2)

      workerProbe1.ignoreMsg {
        case Heartbeat ⇒ true
      }
      workerProbe2.ignoreMsg {
        case Heartbeat ⇒ true
      }

      for (_ ← 1 to 3) {
        // tick-expression says it should be fired each second
        withDelay(1000) {
          workerProbe1.expectMsgClass(timeout, classOf[Work])
          master ! WorkDone(worker1)

          workerProbe2.expectMsgClass(timeout, classOf[Work])
          master ! WorkDone(worker2)
        }
      }

      master.stop()
    }

  }

  "The Master actor testing without tick and heartbeat" should {

    "register worker when got Register message" in new MasterWithoutSchedulersProbeWorkerFixture {
      assert(idleWorkers.isEmpty)

      master ! Register(worker1)
      assert(idleWorkers.size == 1)

      master ! Register(worker1)
      assert(idleWorkers.size == 1) // registering the same

      master ! Register(workerProbe2.ref)
      assert(idleWorkers.size == 2)
      assert(idleWorkers.contains(worker1))
      assert(idleWorkers.contains(worker2))
    }

    "unregister worker" in new MasterWithoutSchedulersRealWorkerFixture {
      master ! Register(worker1)
      master ! Register(worker2)
      assert(idleWorkers.size == 2)

      master ! Unregister(worker1)
      assert(idleWorkers.size == 1)
      assert(!idleWorkers.contains(worker1))

      master ! Unregister(worker1)
      assert(idleWorkers.size == 1) // unregistering the same

      master ! Unregister(worker2)
      assert(idleWorkers.isEmpty)
    }

    "workDone received without pending metrics mark worker as idle" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.idleWorkers = Set[ActorRef]()
      underlyingMaster.pendingMetrics = 0

      master ! WorkDone(worker1)

      assert(idleWorkers.size == 1)
      assert(idleWorkers.contains(worker1))
    }

    "workDone received with pending metrics dispatch Work" in new MasterWithoutSchedulersProbeWorkerFixture {
      val firstMetric = Metric("metric1", "histogram")
      underlyingMaster.idleWorkers = Set()
      underlyingMaster.affinityConsistentHashRing.addWorker(worker1)
      underlyingMaster.affinityConsistentHashRing.assignWorkers(Seq(Metric("metric1", "histogram"), Metric("metric2", "histogram")))
      underlyingMaster.pendingMetrics = 1

      master ! WorkDone(worker1)

      assert(idleWorkers.size == 0)
      workerProbe1.expectMsg(Work(Seq(firstMetric)))
      assert(pendingMetrics == 0)
    }

    "when receive a PendingMetrics message without idle workers nor pending metrics add all metrics as pending" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.pendingMetrics = 0

      val expectedMetrics = Seq(Metric("a", "histogram"), Metric("b", "histogram"), Metric("c", "histogram"), Metric("d", "histogram"), Metric("e", "histogram"))

      master ! PendingMetrics(expectedMetrics)

      assert(pendingMetrics == expectedMetrics.size)
    }

    "when receive a PendingMetrics message with some pending metrics reset and queue the new ones" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.pendingMetrics = 23

      val expectedMetrics = Seq(Metric("a", "histogram"), Metric("b", "histogram"), Metric("c", "histogram"), Metric("d", "histogram"), Metric("e", "histogram"))

      master ! PendingMetrics(expectedMetrics)

      assert(pendingMetrics == expectedMetrics.size)
    }
    //
    //    "when receive a PendingMetrics message with pending metrics and idle workers assign work" in new MasterWithoutSchedulersProbeWorkerFixture {
    //      val allMetrics = Seq(Metric("a", "histogram"), Metric("b", "histogram"), Metric("c", "histogram"), Metric("d", "histogram"), Metric("e", "histogram"))
    //
    //      underlyingMaster.idleWorkers = Set(worker1, worker2)
    //      underlyingMaster.pendingMetrics = 25
    //      underlyingMaster.affinityConsistentHashRing.addWorker(worker1)
    //      underlyingMaster.affinityConsistentHashRing.addWorker(worker2)
    //
    //      master ! PendingMetrics(allMetrics)
    //      workerProbe1.expectMsg(Work(Seq(Metric("a", "histogram"))))
    //      workerProbe2.expectMsg(Work(Seq(Metric("b", "histogram"))))
    //      assert(idleWorkers.isEmpty)
    //      assert(pendingMetrics == 3)
    //
    //      underlyingMaster.idleWorkers = Set(worker1, worker2)
    //      master ! PendingMetrics(allMetrics)
    //      workerProbe1.expectMsg(Work(Seq(Metric("c", "histogram"))))
    //      workerProbe2.expectMsg(Work(Seq(Metric("d", "histogram"))))
    //      assert(idleWorkers.isEmpty)
    //      assert(pendingMetrics == Vector(Metric("e", "histogram"), Metric("a", "histogram"), Metric("b", "histogram")))
    //
    //      underlyingMaster.idleWorkers = Set(worker1, worker2)
    //      master ! PendingMetrics(allMetrics)
    //      workerProbe1.expectMsg(Work(Seq(Metric("e", "histogram"))))
    //      workerProbe2.expectMsg(Work(Seq(Metric("a", "histogram"))))
    //      assert(idleWorkers.isEmpty)
    //      assert(pendingMetrics == Vector(Metric("b", "histogram"), Metric("c", "histogram"), Metric("d", "histogram")))
    //    }

  }

  trait ScheduledMasterProbeWorkerFixture {

    class ScheduledMaster extends Master with WorkerProbeRouterProvider with DummyMetricFinder {
      override def leader: Receive = noRoutees orElse super.leader

      private def noRoutees: Receive = {
        case Routees(routees) ⇒ {
        }
      }
    }

    val master = TestActorRef(Props(new ScheduledMaster() {
      super.initializeLeader()
      override def scheduleCheckLeadership() = None
    }))

    val underlyingMaster = master.underlyingActor.asInstanceOf[ScheduledMaster]

    def workerProbe1 = underlyingMaster.workerProbe1

    def workerProbe2 = underlyingMaster.workerProbe2

    def worker1 = underlyingMaster.worker1

    def worker2 = underlyingMaster.worker2

    def withDelay(sleepMillis: Int)(block: ⇒ Unit) = {
      Thread.sleep(sleepMillis)
      block
    }

  }

  trait MasterWithoutSchedulersRealWorkerFixture {

    class TestMasterWithoutSchedulers extends NoScheduledMaster with RealWorkerRouterProvider with DummyMetricFinder

    val master = TestActorRef(Props(new TestMasterWithoutSchedulers() {
      router = Some(createRouter())
    }).withDispatcher(CallingThreadDispatcher.Id))

    val underlyingMaster = master.underlyingActor.asInstanceOf[TestMasterWithoutSchedulers]

    def worker1 = underlyingMaster.worker1

    def worker2 = underlyingMaster.worker2

    def idleWorkers = underlyingMaster.idleWorkers

  }

  trait MasterWithoutSchedulersProbeWorkerFixture {

    class TestMasterWithoutSchedulers extends NoScheduledMaster with WorkerProbeRouterProvider with DummyMetricFinder

    val master = TestActorRef(Props(new TestMasterWithoutSchedulers() {
      router = Some(createRouter())
    }).withDispatcher(CallingThreadDispatcher.Id))

    val underlyingMaster = master.underlyingActor.asInstanceOf[TestMasterWithoutSchedulers]

    def workerProbe1 = underlyingMaster.workerProbe1

    def workerProbe2 = underlyingMaster.workerProbe2

    def worker1 = underlyingMaster.worker1

    def worker2 = underlyingMaster.worker2

    def idleWorkers = underlyingMaster.idleWorkers

    def pendingMetrics = underlyingMaster.pendingMetrics

  }

  trait WorkerProbeRouterProvider extends RouterProvider {
    this: Actor ⇒

    val workerProbe1 = TestProbe()
    val workerProbe2 = TestProbe()
    val worker1 = workerProbe1.ref
    val worker2 = workerProbe2.ref

    override def createRouter: ActorRef = {
      val paths = List(workerProbe1.ref.path.toStringWithoutAddress, workerProbe2.ref.path.toStringWithoutAddress)
      context.actorOf(RoundRobinGroup(paths).props(), "worker-probe-router")
    }

  }

  trait RealWorkerRouterProvider extends RouterProvider {
    this: Actor ⇒
    var worker1: ActorRef = _
    var worker2: ActorRef = _

    override def createRouter: ActorRef = {
      worker1 = context.actorOf(Worker.props.withDispatcher(CallingThreadDispatcher.Id))
      worker2 = context.actorOf(Worker.props.withDispatcher(CallingThreadDispatcher.Id))

      val paths = List(worker1.path.toStringWithoutAddress, worker2.path.toStringWithoutAddress)
      context.actorOf(RoundRobinGroup(paths).props(), "worker-router")
    }
  }

  trait DummyMetricFinder extends MetricFinder {

    import scala.concurrent.Future

    override def lookupMetrics: Future[Seq[Metric]] = Future(Seq(Metric("a", "histogram"), Metric("b", "histogram"), Metric("c", "histogram"), Metric("d", "histogram"), Metric("e", "histogram")))
  }

  trait NoScheduledMaster extends Master {
    // Overriding schedulers to Nothing
    override def scheduleTick(): Option[ActorRef] = None

    override def scheduleHeartbeat() = None

    override def receive = leader()
  }

}

