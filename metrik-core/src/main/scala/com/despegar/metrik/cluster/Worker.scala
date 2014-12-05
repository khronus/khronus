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

import akka.actor.{ ActorRef, Props, Actor, ActorLogging }
import com.despegar.metrik.cluster.Worker.WorkError
import com.despegar.metrik.model.{ MonitoringSupport, Monitoring, TimeWindowChain, Metric }
import scala.util.control.{ NoStackTrace, NonFatal }
import scala.util.{ Failure, Success }

class Worker extends Actor with ActorLogging with TimeWindowChainProvider with MonitoringSupport {
  import context._

  def receive: Receive = idle

  def idle: Receive = {
    case Heartbeat ⇒
      sender ! Register(self)
      become(ready)
      log.info("Worker ready to work: [{}]", self.path)
  }

  def ready: Receive = {
    case Work(metric)      ⇒ process(metric, sender)
    case WorkError(reason) ⇒ throw reason
    case everythingElse    ⇒ //ignore
  }

  def process(metric: Metric, requestor: ActorRef) {
    log.debug(s"Starting to process: $metric")

    timeWindowChain.process(metric) onComplete {
      case Success(_) ⇒
        log.debug(s"Worker ${self.path} has processed $metric successfully")
        requestor ! WorkDone(self)
      case Failure(NonFatal(reason)) ⇒
        log.error(reason, s"Error processing $metric")
        self ! WorkError(new WorkFailureException(reason.getMessage))
    }
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    incrementCounter("workerRestarts")
    log.info(s"Restarted because of ${reason.getMessage}")
  }
}

object Worker {
  case class WorkError(t: Throwable)
  def props: Props = Props(classOf[Worker])
}

trait TimeWindowChainProvider {
  def timeWindowChain: TimeWindowChain = new TimeWindowChain
}

class WorkFailureException(message: String) extends RuntimeException with NoStackTrace