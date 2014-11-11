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

package com.despegar.metrik.util

import akka.actor._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.despegar.metrik.model.TimeWindow
import com.despegar.metrik.model.HistogramTimeWindow
import com.despegar.metrik.model.CounterTimeWindow

class Settings(config: com.typesafe.config.Config, extendedSystem: ExtendedActorSystem) extends Extension {

  object Master {
    val TickCronExpression = config.getString("metrik.master.tick-expression")
    val DiscoveryStartDelay = FiniteDuration(config.getDuration("metrik.master.discovery-start-delay", MILLISECONDS), MILLISECONDS)
    val DiscoveryInterval = FiniteDuration(config.getDuration("metrik.master.discovery-interval", MILLISECONDS), MILLISECONDS)
  }

  object Http {
    val Interface = config.getString("metrik.endpoint")
    val Port: Int = config.getInt("metrik.port")
  }

  object Window {
    val ExecutionDelay: Long = config.getDuration("metrik.windows.execution-delay", MILLISECONDS)
  }

  object Cassandra {
    private val cassandraCfg = config.getConfig("metrik.cassandra")
    val Cluster = cassandraCfg.getString("cluster")
    val Keyspace = cassandraCfg.getString("keyspace")
    val Port = cassandraCfg.getInt("port")
    val Seeds = cassandraCfg.getString("seeds")
  }

  object Histogram {
    val configuredWindows = config.getDurationList("metrik.histogram.windows", MILLISECONDS).asScala.map(adjustDuration(_))
    val windowDurations = (1 millis) +: configuredWindows
    val timeWindows = windowDurations.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      HistogramTimeWindow(duration, previous, windowDurations.last != duration)
    }.toSeq
  }

  object Counter {
    val configuredWindows = config.getDurationList("metrik.counter.windows", MILLISECONDS).asScala.map(adjustDuration(_))
    val windowDurations = (1 millis) +: configuredWindows
    val timeWindows = windowDurations.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      CounterTimeWindow(duration, previous, windowDurations.last != duration)
    }.toSeq
  }

  private def adjustDuration(durationInMillis: Long): FiniteDuration = {
    durationInMillis match {
      case durationInMillis if durationInMillis < 1000 ⇒ durationInMillis millis
      case durationInMillis if durationInMillis < 60000 ⇒ (durationInMillis millis).toSeconds seconds
      case durationInMillis if durationInMillis < 3600000 ⇒ (durationInMillis millis).toMinutes minutes
      case _ ⇒ (durationInMillis millis).toHours hours
    }
  }

}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  def apply() = super.apply(Metrik.system)

  override def lookup = Settings
  override def createExtension(system: ExtendedActorSystem) = new Settings(system.settings.config, system)
}