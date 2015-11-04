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

package com.searchlight.khronus

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.searchlight.khronus.util.log.Logging
import model.Metric

package object cluster {
  case class Register(worker: ActorRef)
  case class Unregister(worker: ActorRef)
  case class Work(metric: Seq[Metric])
  case class WorkDone(worker: ActorRef)
  case class WorkError(worker: ActorRef)
  case object Heartbeat

  object RouterSupervisorStrategy extends Logging {
    final val restartOnError: SupervisorStrategy = {
      OneForOneStrategy() {
        case _: Exception ⇒ {
          log.error("ERROR ON ROUTER")
          Restart
        }
      }
    }
  }
}

