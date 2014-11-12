package com.despegar.metrik.influx

import akka.actor._
import akka.actor
import akka.event.Logging
import com.despegar.metrik.util.{ MetrikStarted, Register }

class InfluxExtension(private val system: ExtendedActorSystem) extends Extension {
  val log = Logging(system, classOf[InfluxExtension])
  log.info(s"Starting the Metrik(Influx) extension")

  val influxActor = system.actorOf(InfluxActor.props, InfluxActor.Name)

  log.debug(s"Subscribing to MetrikStarted Message")
  val influxSubscriber = system.actorOf(InfluxSubscriber.props(influxActor), "influx-subscriber")

  system.eventStream.subscribe(influxSubscriber, classOf[MetrikStarted])
}

class InfluxSubscriber(influxActor: ActorRef) extends Actor {
  def receive: Receive = {
    case MetrikStarted(handler) ⇒
      handler ! Register(InfluxActor.Path, influxActor)
      InfluxDashboardResolver.initialize
  }
}

object InfluxSubscriber {
  def props(influxActor: ActorRef): Props = Props(new InfluxSubscriber(influxActor))
}

object Influx extends ExtensionId[InfluxExtension] with ExtensionIdProvider {
  override def lookup: ExtensionId[_ <: actor.Extension] = Influx
  override def createExtension(system: ExtendedActorSystem): InfluxExtension = new InfluxExtension(system)
}
