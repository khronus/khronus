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
import akka.routing.{ Broadcast, FromConfig }
import com.searchlight.khronus.model.{ MonitoringSupport, Metric }
import com.searchlight.khronus.store.MetaSupport
import com.searchlight.khronus.util.Settings
import us.theatr.akka.quartz.{ AddCronScheduleFailure, _ }

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

class Master extends Actor with ActorLogging with RouterProvider with MetricFinder with MonitoringSupport {

  import com.searchlight.khronus.cluster.Master._
  import context._

  var heartbeatScheduler: Option[Cancellable] = _
  var tickActorRef: Option[ActorRef] = _
  var idleWorkers = Set[ActorRef]()
  var busyWorkers = Set[ActorRef]()

  var pendingMetrics = 0

  val affinityConsistentHashRing = AffinityConsistentHashRing()

  val settings = Settings.Master

  var start: Long = _

  self ! Initialize

  def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case Initialize ⇒
      log.info(s"Initializing master ${self.path}")
      val router = createRouter()

      heartbeatScheduler = scheduleHeartbeat(router)
      tickActorRef = scheduleTick()

      become(initialized(router))

    case AddCronScheduleFailure(reason) ⇒
      log.error(reason, "Could not schedule tick")
      throw reason

    case everythingElse ⇒ //ignore
  }

  def initialized(router: ActorRef): Receive = {

    case Tick ⇒ lookupMetrics onComplete {
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

  def scheduleHeartbeat(router: ActorRef): Option[Cancellable] = {
    log.info("Scheduling Heartbeat in order to discover workers periodically")
    Some(system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router, Broadcast(Heartbeat)))
  }

  def scheduleTick(): Option[ActorRef] = {
    log.info(s"Scheduling tick at ${settings.TickCronExpression}")
    val tickScheduler = actorOf(Props[QuartzActor])
    tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Tick, reply = true)
    Some(tickScheduler)
  }
}

object Master {
  case object Tick
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
  def lookupMetrics: Future[Seq[Metric]] = metaStore.allActiveMetrics
}