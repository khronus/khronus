package com.despegar.metrik.com.despegar.metrik

import akka.actor._
import akka.routing.{ RoundRobinGroup, RoundRobinPool, Broadcast }
import akka.testkit._
import com.despegar.metrik.cluster.Master.{ Tick, Initialize }
import com.despegar.metrik.cluster._
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfter, Matchers, WordSpecLike }
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{ Future, TimeUnit }
import akka.actor.Actor.Receive
import scala.concurrent.duration._
import com.despegar.metrik.cluster.Work
import com.despegar.metrik.cluster.WorkDone
import akka.routing.RoundRobinGroup
import com.despegar.metrik.cluster.Work
import com.despegar.metrik.cluster.WorkDone
import akka.routing.RoundRobinGroup
import akka.pattern.gracefulStop
import scala.concurrent.Await
import akka.util.BoxedType

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
      |    discovery-interval = 2 seconds
      |  }
      |}
    """.stripMargin
  ))

  override protected def afterAll() = TestKit.shutdownActorSystem(system)

  "The Master actor" should {

    "send a Broadcast to heartbeat all workers every discovery-interval" in new ScheduledMasterProbeWorkerFixture {

      withDelay(1000) {
        // First heartbeat is after discovery-start-delay
        workerProbe1.expectMsg(100 millis, Heartbeat)
        workerProbe2.expectMsg(100 millis, Heartbeat)
      }

      for (_ ← 1 to 2) {
        withDelay(2000) {
          // Other hertbeats are after each discovery-interval
          workerProbe1.expectMsg(100 millis, Heartbeat)
          workerProbe2.expectMsg(100 millis, Heartbeat)
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
          workerProbe1.expectMsgClass(100 millis, classOf[Work])
          master ! WorkDone(worker1)

          workerProbe2.expectMsgClass(100 millis, classOf[Work])
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

    "unregister worker when it is terminated" in new MasterWithoutSchedulersRealWorkerFixture {
      master ! Register(worker1)
      master ! Register(worker2)
      assert(idleWorkers.size == 2)

      worker1 ! PoisonPill
      assert(idleWorkers.size == 1)
      assert(!idleWorkers.contains(worker1))

      worker1 ! PoisonPill
      assert(idleWorkers.size == 1) // unregistering the same

      worker2 ! PoisonPill
      assert(idleWorkers.isEmpty)
    }

    "workDone received without pending metrics mark worker as idle" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.idleWorkers = Set[ActorRef]()
      underlyingMaster.pendingMetrics = Seq[String]()

      master ! WorkDone(worker1)

      assert(idleWorkers.size == 1)
      assert(idleWorkers.contains(worker1))
    }

    "workDone received with pending metrics dispatch Work" in new MasterWithoutSchedulersProbeWorkerFixture {
      val firstMetric = "metric1"
      underlyingMaster.idleWorkers = Set()
      underlyingMaster.pendingMetrics = Seq(firstMetric, "metric2")

      master ! WorkDone(worker1)

      assert(idleWorkers.size == 0)
      workerProbe1.expectMsg(Work(firstMetric))
      assert(pendingMetrics.size == 1)
      assert(!pendingMetrics.contains(firstMetric))
    }

    "tick when there is no iddle workers nor pending metrics add all metrics as pendings" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.pendingMetrics = Seq()

      master ! Tick
      assert(pendingMetrics.size == underlyingMaster.lookupMetrics.size)
      underlyingMaster.lookupMetrics.foreach(x ⇒ pendingMetrics.contains(x))
    }

    "tick when there are some pending metrics queue the rest of the metrics" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.pendingMetrics = Seq("d", "e")

      master ! Tick
      assert(pendingMetrics.size == underlyingMaster.lookupMetrics.size)
      assert(pendingMetrics == Seq("d", "e", "a", "b", "c"))
    }

    "tick when there are pending metrics and idle workers assign work" in new MasterWithoutSchedulersProbeWorkerFixture {
      underlyingMaster.idleWorkers = Set(worker1, worker2)
      underlyingMaster.pendingMetrics = Seq("a", "b", "c", "d", "e")

      master ! Tick
      workerProbe1.expectMsg(Work("a"))
      workerProbe2.expectMsg(Work("b"))
      assert(idleWorkers.isEmpty)
      assert(pendingMetrics == Seq("c", "d", "e"))

      underlyingMaster.idleWorkers = Set(worker1, worker2)
      master ! Tick
      workerProbe1.expectMsg(Work("c"))
      workerProbe2.expectMsg(Work("d"))
      assert(idleWorkers.isEmpty)
      assert(pendingMetrics == Seq("e", "a", "b"))

      underlyingMaster.idleWorkers = Set(worker1, worker2)
      master ! Tick
      workerProbe1.expectMsg(Work("e"))
      workerProbe2.expectMsg(Work("a"))
      assert(idleWorkers.isEmpty)
      assert(pendingMetrics == Seq("b", "c", "d"))
    }

  }

  trait ScheduledMasterProbeWorkerFixture {

    class ScheduledMaster extends Master with WorkerProbeRouterProvider with DummyMetricFinder

    val master = TestActorRef(Props(new ScheduledMaster()))

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

    val master = TestActorRef(Props(new TestMasterWithoutSchedulers()).withDispatcher(CallingThreadDispatcher.Id))

    val underlyingMaster = master.underlyingActor.asInstanceOf[TestMasterWithoutSchedulers]

    def worker1 = underlyingMaster.worker1

    def worker2 = underlyingMaster.worker2

    def idleWorkers = underlyingMaster.idleWorkers

  }

  trait MasterWithoutSchedulersProbeWorkerFixture {

    class TestMasterWithoutSchedulers extends NoScheduledMaster with WorkerProbeRouterProvider with DummyMetricFinder

    val master = TestActorRef(Props(new TestMasterWithoutSchedulers()).withDispatcher(CallingThreadDispatcher.Id))

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
    override def lookupMetrics: Seq[String] = Seq("a", "b", "c", "d", "e")
  }

  trait NoScheduledMaster extends Master {
    // Overriding schedulers to Nothing
    override def scheduleTick() = {}

    override def scheduleHeartbeat(router: ActorRef) = {}
  }

}

