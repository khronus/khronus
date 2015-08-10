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

package com.searchlight.khronus.influx

import akka.actor._
import akka.actor
import akka.event.Logging
import com.searchlight.khronus.influx.service.InfluxActor
import com.searchlight.khronus.influx.store.CassandraDashboards
import com.searchlight.khronus.service.ActorSystemSupport
import com.searchlight.khronus.service.HandShakeProtocol.{ Register, KhronusStarted }

class Influx(system: ExtendedActorSystem) extends Extension {
  val log = Logging(system, classOf[Influx])
  log.info(s"Starting the Khronus(Influx) extension")

  val influxActor = system.actorOf(InfluxActor.props, InfluxActor.Name)

  log.debug(s"Subscribing to KhronusStarted Message")
  val influxSubscriber = system.actorOf(InfluxSubscriber.props(influxActor), "influx-subscriber")

  system.eventStream.subscribe(influxSubscriber, classOf[KhronusStarted])

  object Settings {
    val rf = system.settings.config.getConfig("khronus.cassandra.dashboards").getInt("rf")
  }

}

object Influx extends ExtensionId[Influx] with ExtensionIdProvider {
  def apply() = super.apply(ActorSystemSupport.system)

  override def lookup: ExtensionId[_ <: actor.Extension] = Influx
  override def createExtension(system: ExtendedActorSystem): Influx = new Influx(system)
}

class InfluxSubscriber(influxActor: ActorRef) extends Actor with ActorLogging {
  def receive: Receive = {
    case KhronusStarted(handler) ⇒
      log.info("Influx received KhronusStarted")
      val c = CassandraDashboards
      handler ! Register(InfluxActor.Path, influxActor)
  }
}

object InfluxSubscriber {
  def props(influxActor: ActorRef): Props = Props(classOf[InfluxSubscriber], influxActor)
}

