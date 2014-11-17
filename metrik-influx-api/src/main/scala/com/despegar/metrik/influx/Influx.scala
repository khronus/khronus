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

package com.despegar.metrik.influx

import akka.actor._
import akka.actor
import akka.event.Logging
import com.despegar.metrik.influx.finder.InfluxDashboardResolver
import com.despegar.metrik.influx.service.InfluxActor
import com.despegar.metrik.service.HandShakeProtocol.{ Register, MetrikStarted }

class Influx(system: ExtendedActorSystem) extends Extension {
  val log = Logging(system, classOf[Influx])
  log.info(s"Starting the Metrik(Influx) extension")

  val influxActor = system.actorOf(InfluxActor.props, InfluxActor.Name)

  log.debug(s"Subscribing to MetrikStarted Message")
  val influxSubscriber = system.actorOf(InfluxSubscriber.props(influxActor), "influx-subscriber")

  system.eventStream.subscribe(influxSubscriber, classOf[MetrikStarted])
}

object Influx extends ExtensionId[Influx] with ExtensionIdProvider {
  override def lookup: ExtensionId[_ <: actor.Extension] = Influx
  override def createExtension(system: ExtendedActorSystem): Influx = new Influx(system)
}

class InfluxSubscriber(influxActor: ActorRef) extends Actor {
  def receive: Receive = {
    case MetrikStarted(handler) ⇒
      handler ! Register(InfluxActor.Path, influxActor)
      InfluxDashboardResolver.initialize
  }
}

object InfluxSubscriber {
  def props(influxActor: ActorRef): Props = Props(classOf[InfluxSubscriber], influxActor)
}

