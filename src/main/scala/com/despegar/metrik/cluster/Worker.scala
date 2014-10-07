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

import akka.actor.{ Actor, ActorLogging }

class Worker extends Actor with ActorLogging {
  import context._

  def receive: Receive = idle

  def idle: Receive = {
    case DiscoverWorkers ⇒
      sender ! Register(self)
      become(ready)
      log.info("Worker ready to work: [{}]", self.path)
  }

  def ready: Receive = {
    case Work(metric) ⇒
      log.info("Starting to process Metric: [{}]", metric)
      Thread.sleep(10000)
      sender() ! WorkDone(self)

    case everythingElse ⇒ //ignore
  }
}
