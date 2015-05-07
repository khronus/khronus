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

package com.despegar.khronus.cluster

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.routing.{Broadcast, FromConfig}
import com.despegar.khronus.model.{Metric, MonitoringSupport}
import com.despegar.khronus.store.{LeaderElection, MetaSupport}
import com.despegar.khronus.util.Settings
import com.typesafe.config.ConfigFactory
import us.theatr.akka.quartz.{AddCronScheduleFailure, _}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class Master extends Actor with ActorLogging with RouterProvider with MetricFinder with MonitoringSupport {

  import com.despegar.khronus.cluster.Master._
  import context._

  var heartbeatScheduler: Option[Cancellable] = _
  var tickActorRef: Option[ActorRef] = _
  var checkLeadershipScheduler: Option[ActorRef] = _
  var idleWorkers = Set[ActorRef]()
  var busyWorkers = Set[ActorRef]()

  var pendingMetrics = Vector[Metric]()

  val settings = Settings.Master
  
  var router: Option[ActorRef] = _

  var hasLeadership = false

  var checkLeadershipErrorCount = new AtomicInteger(0)
  val MAX_CHECKLEADER_ERROR_COUNT = 2

  var start: Long = _

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
    checkLeadershipScheduler = scheduleCheckLeadership()
    
    router = Some(createRouter())

    heartbeatScheduler = scheduleHeartbeat()
    tickActorRef = scheduleTick()

    become(leader())
  }

  private def initializeBackupLeader: Unit = {
    log.info(s"Initializing backup leader ${self.path}")
    hasLeadership = false

    releaseResources()

    checkLeadershipScheduler = scheduleCheckLeadership()
    become(backupLeader)
  }

  def backupLeader(): Receive = {
    case CheckLeadership => {
      if (hasLeadership) {
        log.error("A backup leader could not have the leader mark as true. Marked as false and continue")
        hasLeadership = false
      }

      LeaderElection.leaderElectionStore.acquireLock() onComplete {
        case Success(election) if (election) => log.debug("backupLeader has succeed in leaderElection"); initializeLeader
        case Failure(ex) => log.error("Error trying to check for leader")
      }
    }
  }

  def leader(): Receive = {

    case CheckLeadership => {
      LeaderElection.leaderElectionStore.renewLock() onComplete {
        case Success(election) if (!election) => log.error("Lost leadership!! Change to backupLeader"); initializeBackupLeader
        case Success(election) if (election) => log.debug("Renew leadership successful")
        case Failure(ex) => {
          if (checkLeadershipErrorCount.incrementAndGet() > MAX_CHECKLEADER_ERROR_COUNT) {
            log.error("Exceed maximum number of errors in update leadership. Change to backupLeader")
            initializeBackupLeader
          } else {
            log.error("Error trying to check for leader")
          }
        }
      }
    }

    case Tick ⇒ lookupMetrics onComplete {
      case Success(metrics) ⇒ self ! PendingMetrics(metrics)
      case Failure(NonFatal(reason)) ⇒ log.error(reason, "Error trying to get metrics.")
    }

    case PendingMetrics(metrics) ⇒
      recordSystemMetrics(metrics)

      val sortedPendingMetrics = collection.SortedSet(pendingMetrics: _*)(Ordering[String].on[Metric] {
        _.name
      })

      pendingMetrics ++= metrics filterNot (metric ⇒ sortedPendingMetrics(metric))

      if (busyWorkers.nonEmpty) log.warning(s"There are still busy workers from previous Tick: $busyWorkers. This may mean that either workers are still processing metrics or Terminated message has not been received yet")
      else start = System.currentTimeMillis()

      while (pendingMetrics.nonEmpty && idleWorkers.nonEmpty) {

        val (currentBatch, pending) = pendingMetrics.splitAt(settings.WorkerBatchSize)

        val worker = idleWorkers.head

        log.debug(s"Dispatching ${currentBatch.mkString(",")} to ${worker.path}")
        incrementCounter("dispatch")

        worker ! Work(currentBatch)

        busyWorkers += worker
        idleWorkers = idleWorkers.tail
        pendingMetrics = pending
      }


    case Register(worker) ⇒
      log.info("Registering worker [{}]", worker.path)
      watch(worker)
      idleWorkers += worker
      removeBusyWorker(worker)

    case WorkDone(worker) ⇒
      if (pendingMetrics.nonEmpty) {
        val (currentBatch, pending) = pendingMetrics.splitAt(settings.WorkerBatchSize)

        log.debug(s"Fast-Dispatching ${currentBatch.mkString(",")} to ${worker.path}")
        incrementCounter("fastDispatch")
        worker ! Work(currentBatch)
        pendingMetrics = pending
      } else {
        log.debug(s"Pending metrics is empty. Adding worker ${worker.path} to worker idle list")
        idleWorkers += worker
        removeBusyWorker(worker)
      }

    case Terminated(worker) ⇒
      log.info("Removing worker [{}] from worker list", worker.path)
      idleWorkers -= worker
      if (busyWorkers.contains(worker)) {
        removeBusyWorker(worker)
      }
  }

  private def releaseResources(): Unit = {
    log.info("Releasing resources in Master Actor")
    router map (router => stop(router))
    heartbeatScheduler.map { case scheduler: Cancellable ⇒ scheduler.cancel() }
    tickActorRef.map { case actor: ActorRef ⇒ stop(actor) }
    checkLeadershipScheduler.map { case actor: ActorRef ⇒ stop(actor) }

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

    log.info(s"Metrics received: $metricsSize, Pending metrics: ${pendingMetrics.size}, ${idleWorkers.size} idle workers, ${busyWorkers.size} busy workers")
    log.debug(s"Pending metrics: $pendingMetrics workers idle: $idleWorkers")

    recordGauge("idleWorkers", idleWorkers.size)
    recordGauge("pendingMetrics", pendingMetrics.size)
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
        log.info(s"Total time spent in Tick: $timeElapsed ms")
      }
    }
  }

  override def postStop(): Unit = {
    log.warning("Master postStop invoked!")
    super.postStop()

    releaseResources()
  }

  def scheduleHeartbeat(): Option[Cancellable] = {
    log.info("Scheduling Heartbeat in order to discover workers periodically")
    Some(system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router.get, Broadcast(Heartbeat)))
  }

  def scheduleTick(): Option[ActorRef] = {
    log.info(s"Scheduling tick at ${settings.TickCronExpression}")
    val tickScheduler = actorOf(Props[QuartzActor])
    tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Tick, reply = true)
    Some(tickScheduler)
  }

  def scheduleCheckLeadership(): Option[ActorRef] = {
    log.info(s"Scheduling checkForLeadership message at ${settings.TickCronExpression}")
    val scheduler = actorOf(Props[QuartzActor])
    scheduler ! AddCronSchedule(self, settings.TickCronExpression, CheckLeadership, reply = true)
    Some(scheduler)
  }
}

object Master {
  case object Tick
  case object CheckLeadership
  case class PendingMetrics(metrics: Seq[Metric])
  case class Initialize(cronExpression: String, router: ActorRef)
  case class MasterConfig(cronExpression: String)

  def props: Props = Props(classOf[Master])
}

trait RouterProvider {
  this: Actor ⇒

  def createRouter(): ActorRef = {
    context.actorOf(Props[Worker].withRouter(FromConfig().withSupervisorStrategy(RouterSupervisorStrategy.restartOnError)), "workerRouter")
  }
}

trait MetricFinder extends MetaSupport {
  def lookupMetrics: Future[Seq[Metric]] = metaStore.allMetrics
}

