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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.routing.{ Broadcast, FromConfig }
import com.searchlight.khronus.model.{Tick, MonitoringSupport, Metric}
import com.searchlight.khronus.store.{LeaderElection, MetaSupport}
import com.searchlight.khronus.util.Settings
import us.theatr.akka.quartz.{ AddCronScheduleFailure, _ }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

class Master extends Actor with ActorLogging with RouterProvider with MetricFinder with MonitoringSupport {

  import com.searchlight.khronus.cluster.Master._
  import context._

  var heartbeatScheduler: Option[Cancellable] = None
  var tickActorRef: Option[ActorRef] = None
  var idleWorkers = Set[ActorRef]()
  var busyWorkers = Set[ActorRef]()
  var checkLeadershipScheduler: Option[Cancellable] = None

  var pendingMetrics = 0

  val affinityConsistentHashRing = AffinityConsistentHashRing()

  val settings = Settings.Master

  var start: Long = _

  var hasLeadership = false
  var checkLeadershipErrorCount = new AtomicInteger(0)
  val MAX_CHECKLEADER_ERROR_COUNT = 2

  var router: Option[ActorRef] = None

  self ! Initialize

  def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case Initialize ⇒ LeaderElection.leaderElectionStore.acquireLock() onComplete {
      case Success(acquire) => {
        if (acquire) {
          initializeLeader
        } else {
          initializeBackupLeader
        }
      }
      case Failure(ex) => {
        log.error("Error trying to check for leader. Schedule to re initialize in 10 seconds")
        system.scheduler.scheduleOnce(10 seconds, self, Initialize)
      }
    }

    case AddCronScheduleFailure(reason) ⇒
      log.error(reason, "Could not schedule tick")
      throw reason

    case everythingElse ⇒ //ignore
  }

  private def initializeLeader: Unit = {
      log.info(s"Initializing leader ${self.path}")
      hasLeadership = true
      checkLeadershipErrorCount.set(0)
      if (checkLeadershipScheduler == null || !checkLeadershipScheduler.isDefined){
        checkLeadershipScheduler = scheduleCheckLeadership()
      }

      router = Some(createRouter())

      heartbeatScheduler = scheduleHeartbeat()
      tickActorRef = scheduleTick()

      incrementCounter("leader")
      become(leader())
    }

  private def initializeBackupLeader: Unit = {
    log.info(s"Initializing backup leader ${self.path}")
    hasLeadership = false

    releaseResources()

    if (checkLeadershipScheduler == null || !checkLeadershipScheduler.isDefined){
      checkLeadershipScheduler = scheduleCheckLeadership()
    }

    incrementCounter("buckupLeader")
    become(backupLeader)
  }

  def backupLeader(): Receive = {
    case CheckLeadership => {
      if (hasLeadership) {
        log.error("A backup leader could not have the leader mark as true. Marked as false and continue")
        hasLeadership = false
      }

      LeaderElection.leaderElectionStore.acquireLock() onComplete {
        case Success(election) if (election) => {
          log.info("backupLeader has succeed in leaderElection")
          incrementCounter("buckupLeaderElectionSucess")
          initializeLeader
        }
        case Success(election) if (!election) => {
          log.info("backupLeader could not acquiere lock")
          incrementCounter("backupLeaderLostElection")
        }
        case Failure(ex) => {
          log.error("Error trying to check for leader")
          incrementCounter("buckupLeaderErrorElection")
        }
      }
    }

    case Terminated(child) => log.info(s"Receive terminated on Master from ${child.path}")
  }



  def leader(): Receive = {

    case CheckLeadership => {
      LeaderElection.leaderElectionStore.renewLock() onComplete {
        case Success(election) if (!election) => {
          log.error("Lost leadership!! Change to backupLeader")
          incrementCounter("leaderLostRenew")
          initializeBackupLeader
        }
        case Success(election) if (election) => {
          log.info("Renew leadership successful")
          incrementCounter("leaderSuccessRenew")
        }
        case Failure(ex) => {
          if (checkLeadershipErrorCount.incrementAndGet() > MAX_CHECKLEADER_ERROR_COUNT) {
            log.error("Exceed maximum number of errors in update leadership. Change to backupLeader")
            incrementCounter("leaderChangeToBuckupOnError")
            initializeBackupLeader
          } else {
            log.error("Error trying to check for leader")
          }
        }
      }
    }

    case Master.Tick ⇒ lookupMetrics onComplete {
      case Success(metrics)          ⇒ self ! PendingMetrics(metrics)
      case Failure(NonFatal(reason)) ⇒ log.error(reason, "Error trying to get metrics.")
    }

    case PendingMetrics(metrics) ⇒
      recordSystemMetrics(metrics)

      var metricsToProcess = metrics.toSet

      if (pendingMetrics > 0) {
        log.warning(s"There are still ${pendingMetrics} from previous Tick. Merging previous with current metrics")
        //recover pending metrics
        metricsToProcess ++= affinityConsistentHashRing.remainingMetrics()
      }

      pendingMetrics = metricsToProcess.size

      affinityConsistentHashRing.assignWorkers(metricsToProcess.toSeq)

      if (busyWorkers.nonEmpty) log.warning(s"There are still ${busyWorkers.size} busy workers from previous Tick. This may mean that either workers are still processing metrics or Terminated message has not been received yet. Workers $busyWorkers")
      else start = System.currentTimeMillis()

      while (pendingMetrics > 0 && idleWorkers.nonEmpty) {
        val worker = idleWorkers.head

        val metrics = dispatch(worker, "dispatch")

        if (metrics.nonEmpty) busyWorkers += worker

        idleWorkers = idleWorkers.tail
      }

    case Register(worker) ⇒
      log.info("Registering Worker [{}]", worker.path.name)
      watch(worker)
      idleWorkers += worker
      affinityConsistentHashRing.addWorker(worker)
      removeBusyWorker(worker)

    case WorkDone(worker) ⇒
      if (pendingMetrics > 0 && affinityConsistentHashRing.hasPendingMetrics(worker)) {
        dispatch(worker, "fastDispatch")
      } else {
        log.debug(s"Pending metrics is 0. Adding worker ${worker.path} to worker idle list")
        idleWorkers += worker
        removeBusyWorker(worker)
      }

    case Terminated(worker) ⇒
      log.info("Removing worker [{}] from worker list", worker.path)
      idleWorkers -= worker
      affinityConsistentHashRing.removeWorker(worker)
      removeBusyWorker(worker)

    case WorkError(worker) ⇒
      idleWorkers += worker
      removeBusyWorker(worker)

  }

  private def dispatch(worker: ActorRef, dispatchType: String): Seq[Metric] = {
    val metrics = affinityConsistentHashRing.nextMetrics(worker)

    log.debug(s"$dispatchType ${metrics.mkString(",")} to ${worker.path}")
    incrementCounter(dispatchType)

    worker ! Work(metrics)
    pendingMetrics -= metrics.size

    metrics
  }

  private def releaseResources(): Unit = {
    log.info("Releasing resources in Master Actor")
    this.idleWorkers map (w => stop(_))
    this.idleWorkers = Set[ActorRef]()
    this.busyWorkers map (stop(_))
    this.busyWorkers = Set[ActorRef]()
    router map(r => {
      r ! Broadcast(PoisonPill)
      stop(r)
    })
    heartbeatScheduler.map { case scheduler: Cancellable ⇒ scheduler.cancel() }
    tickActorRef.map { case actor: ActorRef ⇒ stop(actor) }
    //checkLeadershipScheduler.map { case actor: ActorRef ⇒ stop(actor) }
    checkLeadershipErrorCount.set(0)

    freeLeadership()
  }

  private def freeLeadership(): Future[Boolean] = {
    val f = LeaderElection.leaderElectionStore.releaseLock()

    f onComplete {
      case Success(freeLock) => log.info(s"Release lock result: $freeLock")
      case Failure(ex) => log.error("Error releasing the lock", ex)
    }

    f
  }

  private def recordSystemMetrics(metrics: Seq[Metric]) {
    val metricsSize = metrics.size

    log.info(s"Starting Tick. [metrics=$metricsSize,pending=${pendingMetrics},idle-workers=${idleWorkers.size},busy-workers=${busyWorkers.size}]")
    log.debug(s"Pending metrics: $pendingMetrics workers idle: $idleWorkers")

    recordGauge("idleWorkers", idleWorkers.size)
    recordGauge("pendingMetrics", pendingMetrics)
    recordGauge("metrics", metricsSize)
    recordGauge("metricsReceived", metricsSize)
  }

  private def removeBusyWorker(worker: ActorRef) = {
    if (busyWorkers.contains(worker)) {
      busyWorkers -= worker
      if (busyWorkers.isEmpty) {
        //no more busy workers. end of the tick
        val timeElapsed = System.currentTimeMillis() - start
        recordTime("totalTimeTick", timeElapsed)
        log.info(s"Finished Tick. [elapsed-time=${timeElapsed}ms]")
      }
    }
  }

  override def postStop(): Unit = {
    super.postStop()

    log.info("Cancelling heartbeat scheduler")
    heartbeatScheduler.map { case scheduler: Cancellable ⇒ scheduler.cancel() }

    log.info("Stopping tick actor ref")
    tickActorRef.map { case actor: ActorRef ⇒ stop(actor) }
  }

  def scheduleHeartbeat(): Option[Cancellable] = {
    log.info("Scheduling Heartbeat in order to discover workers periodically")
    Some(system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router.get, Broadcast(Heartbeat)))
  }

  def scheduleTick(): Option[ActorRef] = {
    log.info(s"Scheduling tick at ${settings.TickCronExpression}")
    val tickScheduler = actorOf(Props[QuartzActor])
    tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Master.Tick, reply = true)
    Some(tickScheduler)
  }

    def scheduleCheckLeadership(): Option[Cancellable] = {
      log.info(s"Scheduling checkForLeadership message at ${settings.CheckLeaderCronExpression}")
      //val scheduler = actorOf(Props[QuartzActor])
      //scheduler ! AddCronSchedule(self, settings.CheckLeaderCronExpression, CheckLeadership, reply = true)
      //Some(scheduler)
      Some(system.scheduler.schedule(0 seconds, 10 seconds, self, CheckLeadership))
    }
}

object Master {
  case object Tick
  case class PendingMetrics(metrics: Seq[Metric])
  case class Initialize(cronExpression: String, router: ActorRef)
  case object CheckLeadership
  case class MasterConfig(cronExpression: String)

  def props: Props = Props(classOf[Master])
}

trait RouterProvider {
  this: Actor ⇒

  def createRouter(): ActorRef = {
    context.actorOf(Props[Worker].withRouter(FromConfig().withSupervisorStrategy(RouterSupervisorStrategy.restartOnError)), "RandomPoolActor")
  }
}

trait MetricFinder extends MetaSupport {
  def lookupMetrics: Future[Seq[Metric]] = metaStore.allActiveMetrics
}