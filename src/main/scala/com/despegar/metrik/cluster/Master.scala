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
import com.despegar.metrik.util.Settings
import us.theatr.akka.quartz.{ AddCronScheduleFailure, _ }

class Master extends Actor with ActorLogging {

  import Master._
  import context._

  var idleWorkers = Set[ActorRef]()
  var pendingMetrics = Seq[String]()

  val settings = Settings(system).Master

  self ! Initialize

  def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case Initialize ⇒
      val router = actorOf(Props[Worker].withRouter(FromConfig()), "workerRouter")
      val tickScheduler = actorOf(Props[QuartzActor])

      system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router, Broadcast(DiscoverWorkers))
      tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Tick, true)

      become(initialized(router))

    case AddCronScheduleFailure(reason) ⇒ throw reason
    case everythingElse                 ⇒ //ignore
  }

  def metrics: Seq[String] = Seq("a", "b", "c", "d", "e")

  def initialized(router: ActorRef): Receive = {
    case Tick ⇒
      pendingMetrics ++= metrics filterNot (metric ⇒ pendingMetrics contains metric)

      while (pendingMetrics.nonEmpty && idleWorkers.nonEmpty) {
        val worker = idleWorkers.head
        val pending = pendingMetrics.head

        worker ! Work(pending)

        idleWorkers = idleWorkers.tail
        pendingMetrics = pendingMetrics.tail
      }

    case Register(worker) ⇒
      log.info("Registring worker [{}]", worker.path)
      watch(worker)
      idleWorkers += worker

    case WorkDone(worker) ⇒
      if (pendingMetrics.nonEmpty) {
        worker ! Work(pendingMetrics.head)
        pendingMetrics = pendingMetrics.tail
      } else {
        idleWorkers += worker
      }

    case Terminated(worker) ⇒
      log.info("Removing worker [{}] from worker list", worker.path)
      idleWorkers -= worker
  }
}

object Master {
  case object Tick
  case class Initialize(cronExpression: String, router: ActorRef)
  case class MasterConfig(cronExpression: String)

  def props: Props = Props(classOf[Master])
}