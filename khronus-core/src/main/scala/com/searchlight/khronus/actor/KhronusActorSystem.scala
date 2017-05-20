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

package com.searchlight.khronus.actor

import akka.actor.ActorSystem
import akka.io.IO
import com.searchlight.khronus.actor.HandShakeProtocol.{ KhronusStarted, Register }
import com.searchlight.khronus.controller.{ ExplorerEndpoint, QueryEndpoint, MetricsEndpoint }
import com.searchlight.khronus.util.Settings
import com.searchlight.khronus.util.log.Logging
import spray.can.Http

trait KhronusActorSystem {
  implicit def system = KhronusActorSystem.system

  val handlerActor = system.actorOf(KhronusHandler.props, KhronusHandler.Name)

  IO(Http) ! Http.Bind(handlerActor, Settings.Http.Interface, Settings.Http.Port)

  val metricsEndpointActor = system.actorOf(MetricsEndpoint.props, MetricsEndpoint.Name)
  val queryEndpointActor = system.actorOf(QueryEndpoint.props, QueryEndpoint.Name)
  val explorerEndpointActor = system.actorOf(ExplorerEndpoint.props, ExplorerEndpoint.Name)

  handlerActor ! Register(MetricsEndpoint.Path, metricsEndpointActor)
  handlerActor ! Register(QueryEndpoint.Path, queryEndpointActor)
  handlerActor ! Register(ExplorerEndpoint.Path, explorerEndpointActor)

  system.eventStream.publish(KhronusStarted(handlerActor))
}

object KhronusActorSystem extends Logging {
  val system = ActorSystem("khronus-system")

  sys.addShutdownHook({
    log.info("Shutting down khronus actor system")
    system.terminate()
  })

}