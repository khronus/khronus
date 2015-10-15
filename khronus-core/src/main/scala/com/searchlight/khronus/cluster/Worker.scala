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
import com.searchlight.khronus.model.{ Metric, MonitoringSupport, TimeWindowChain }

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
    case Work(metric)   ⇒ process(metric, sender())
    case everythingElse ⇒ //ignore
  }

  def process(metrics: Seq[Metric], requestor: ActorRef): Unit = {
    log.debug(s"Worker ${self.path.name} starting to process ${metrics.size} metrics")
    log.debug(s"Starting to process: ${metrics.mkString(",")}")

    timeWindowChain.process(metrics).onComplete {
      case Success(_) ⇒
        log.debug(s"Worker ${self.path.name} has processed ${metrics.size} metrics successfully")
        log.debug(s"Worker ${self.path} has processed ${metrics.mkString(",")} successfully")
        incrementCounter("workerDone")
        requestor ! WorkDone(self)

      case Failure(NonFatal(reason)) ⇒
        incrementCounter("workerErrors")
        log.error(reason, s"(${reason.getMessage}}) Error processing ${metrics.splitAt(20)._1.mkString(",")}")
        requestor ! WorkError(self)
    }
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    incrementCounter("workerRestarts")
    log.info(s"Restarted because of ${reason.getMessage}")
  }

  override def postStop(): Unit = {
    log.info(s"Stop worker ${this.self.path}")
    super.postStop()
  }

  override def unhandled(message: Any): Unit = {
    log.warning(s"Unhandled message ${this.self.path}")
  }
}

object Worker {
  def props: Props = Props(classOf[Worker])
}

trait TimeWindowChainProvider {
  def timeWindowChain: TimeWindowChain = new TimeWindowChain
}

class WorkFailureException(message: String) extends RuntimeException(message) with NoStackTrace
