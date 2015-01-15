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

import akka.actor._
import akka.routing.{ Broadcast, FromConfig }
import com.despegar.khronus.model.{ Metric, MonitoringSupport }
import com.despegar.khronus.store.MetaSupport
import com.despegar.khronus.util.Settings
import us.theatr.akka.quartz.{ AddCronScheduleFailure, _ }

import scala.collection.SortedSet
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

class Master extends Actor with ActorLogging with RouterProvider with MetricFinder with MonitoringSupport {

  import com.despegar.khronus.cluster.Master._
  import context._

  var heartbeatScheduler: Option[Cancellable] = _
  var tickActorRef: Option[ActorRef] = _
  var idleWorkers = Set[ActorRef]()
  var busyWorkers = Set[ActorRef]()

  var pendingMetrics = Vector[Metric]()

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

      val sortedPendingMetrics = sortPendingMetrics()

      pendingMetrics ++= metrics filterNot (metric ⇒ sortedPendingMetrics(metric))

      if (busyWorkers.nonEmpty) log.warning(s"There are still ${busyWorkers.size} busy workers from previous Tick. This may mean that either workers are still processing metrics or Terminated message has not been received yet")
      else start = System.currentTimeMillis()

      idleWorkers = idleWorkers.toSeq.sorted.toSet

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

  private def recordSystemMetrics(metrics: Seq[Metric]) {
    val metricsSize = metrics.size

    log.info(s"Starting Tick. Metrics received: $metricsSize, Pending metrics from previous Tick: ${pendingMetrics.size}, ${idleWorkers.size} idle workers, ${busyWorkers.size} busy workers")
    log.debug(s"Pending metrics: $pendingMetrics workers idle: $idleWorkers")
    log.debug(s"Idle workers: $idleWorkers")

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

  private def sortPendingMetrics(): SortedSet[Metric] = {
    collection.SortedSet(pendingMetrics: _*)(Ordering[String].on[Metric] {
      _.name
    })
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
  def lookupMetrics: Future[Seq[Metric]] = metaStore.allMetrics
}
