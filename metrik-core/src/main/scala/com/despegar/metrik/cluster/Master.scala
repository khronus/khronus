/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.cluster

import akka.actor._
import akka.routing.{ Broadcast, FromConfig }
import com.despegar.metrik.model.{ MonitoringSupport, Metric, Monitoring }
import com.despegar.metrik.store.MetaSupport
import com.despegar.metrik.util.Settings
import us.theatr.akka.quartz.{ AddCronScheduleFailure, _ }

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

class Master extends Actor with ActorLogging with RouterProvider with MetricFinder with MonitoringSupport {

  import com.despegar.metrik.cluster.Master._
  import context._

  var heartbeatScheduler: Option[Cancellable] = _
  var tickActorRef: Option[ActorRef] = _
  var idleWorkers = Set[ActorRef]()
  var busyWorkers = Set[ActorRef]()

  var pendingMetrics = Seq[Metric]()

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

    case PendingMetrics(metrics) ⇒ {
      val metricsSize = metrics.size
      log.info(s"Metrics received: $metricsSize, Pending metrics: ${pendingMetrics.size} and ${idleWorkers.size} idle workers")
      log.debug(s"Pending metrics: $pendingMetrics workers idle: $idleWorkers")
      log.debug(s"Idle workers: $idleWorkers")

      recordGauge("idleWorkers", idleWorkers.size)
      recordGauge("pendingMetrics", pendingMetrics.size)
      recordGauge("metrics", metricsSize)
      recordGauge("metricsReceived", metricsSize)

      pendingMetrics ++= metrics filterNot (metric ⇒ pendingMetrics contains metric)

      if (busyWorkers.nonEmpty) {
        log.warning(s"There are still busy workers from previous Tick: $busyWorkers. This may mean that either workers are still processing metrics or Terminated message has not been received yet")
      } else {
        start = System.currentTimeMillis()
      }

      while (pendingMetrics.nonEmpty && idleWorkers.nonEmpty) {
        val worker = idleWorkers.head
        val pending = pendingMetrics.head

        log.info(s"Dispatching $pending to ${worker.path}")
        incrementCounter("dispatch")
        worker ! Work(pending)

        busyWorkers += worker
        idleWorkers = idleWorkers.tail
        pendingMetrics = pendingMetrics.tail
      }
    }

    case Register(worker) ⇒
      log.info("Registering worker [{}]", worker.path)
      watch(worker)
      idleWorkers += worker
      removeBusyWorker(worker)

    case WorkDone(worker) ⇒
      if (pendingMetrics.nonEmpty) {
        val pending = pendingMetrics.head
        log.info(s"Fast-Dispatching $pending to ${worker.path}")
        incrementCounter("fastDispatch")
        worker ! Work(pending)
        pendingMetrics = pendingMetrics.tail
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
    super.postStop()

    log.info("Cancelling heartbeat scheduler")
    heartbeatScheduler.map({ case scheduler: Cancellable ⇒ scheduler.cancel() })

    log.info("Stopping tick actor ref")
    tickActorRef.map({ case actor: ActorRef ⇒ stop(actor) })

  }

  def scheduleHeartbeat(router: ActorRef): Option[Cancellable] = {
    log.info("Scheduling Heartbeat in order to discover workers periodically")
    Some(system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router, Broadcast(Heartbeat)))
  }

  def scheduleTick(): Option[ActorRef] = {
    log.info(s"Scheduling tick at ${settings.TickCronExpression}")
    val tickScheduler = actorOf(Props[QuartzActor])
    tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Tick, true)
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
