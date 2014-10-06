package com.despegar.metrik.cluster

import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class Settings(config: Config, extendedSystem: ExtendedActorSystem) extends Extension {

  object Master {
    val TickCronExpression = config.getString("metrik.master.tick-expression")
    val DiscoveryStartDelay = FiniteDuration(config.getDuration("metrik.master.discovery-start-delay",TimeUnit.MILLISECONDS),TimeUnit.MILLISECONDS)
    val DiscoveryInterval = FiniteDuration(config.getDuration("metrik.master.discovery-interval", TimeUnit.MILLISECONDS),TimeUnit.MILLISECONDS)
  }

  object Http {
    val Host = ""
    val Port = ""
  }
}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {
  override def lookup = Settings
  override def createExtension(system: ExtendedActorSystem) =  new Settings(system.settings.config, system)
}

