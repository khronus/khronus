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
import com.despegar.metrik.model.{ MetricType, CounterTimeWindow, HistogramTimeWindow }
import com.despegar.metrik.service.ActorSystemSupport

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ FiniteDuration, _ }

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

  object Dashboard {
    val MinResolutionPoints: Int = config.getInt("metrik.dashboards.min-resolution-points")
    val MaxResolutionPoints: Int = config.getInt("metrik.dashboards.max-resolution-points")
  }

  object Cassandra {
    private val cassandraCfg = config.getConfig("metrik.cassandra")
    val ClusterName = cassandraCfg.getString("cluster")
    val Keyspace = cassandraCfg.getString("keyspace")
    val MaxConnectionsPerHost = cassandraCfg.getInt("max-connections-per-host")
    val SocketTimeout = cassandraCfg.getDuration("socket-timeout", MILLISECONDS).toInt
    val ConnectionTimeout = cassandraCfg.getDuration("connection-timeout", MILLISECONDS).toInt
    val Port = cassandraCfg.getInt("port")
    val Seeds = cassandraCfg.getString("seeds").split(",").toSeq
  }

  object Histogram {
    private val histogramConfig = config.getConfig("metrik.histogram")

    val ConfiguredWindows = histogramConfig.getDurationList("windows", MILLISECONDS).asScala.map(adjustDuration(_))
    val BucketRetentionPolicy = histogramConfig.getDuration("bucket-retention-policy", SECONDS).toInt
    val SummaryRetentionPolicy = histogramConfig.getDuration("summary-retention-policy", SECONDS).toInt
    val WindowDurations = (1 millis) +: ConfiguredWindows
    val TimeWindows = WindowDurations.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      HistogramTimeWindow(duration, previous, WindowDurations.last != duration)
    }.toSeq

    val BucketLimit = histogramConfig.getInt("bucket-limit")
    val BucketFetchSize = histogramConfig.getInt("bucket-fetch-size")
    val SummaryLimit = histogramConfig.getInt("summary-limit")
    val SummaryFetchSize = histogramConfig.getInt("summary-fetch-size")

  }

  object Counter {
    private val counterConfig = config.getConfig("metrik.counter")

    val ConfiguredWindows = counterConfig.getDurationList("windows", MILLISECONDS).asScala.map(adjustDuration(_))
    val BucketRetentionPolicy = counterConfig.getDuration("bucket-retention-policy", SECONDS).toInt
    val SummaryRetentionPolicy = counterConfig.getDuration("summary-retention-policy", SECONDS).toInt
    val WindowDurations = (1 millis) +: ConfiguredWindows
    val TimeWindows = WindowDurations.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      CounterTimeWindow(duration, previous, WindowDurations.last != duration)
    }.toSeq

    val BucketLimit = counterConfig.getInt("bucket-limit")
    val BucketFetchSize = counterConfig.getInt("bucket-fetch-size")
    val SummaryLimit = counterConfig.getInt("summary-limit")
    val SummaryFetchSize = counterConfig.getInt("summary-fetch-size")
  }

  private def adjustDuration(durationInMillis: Long): FiniteDuration = {
    durationInMillis match {
      case durationInMillis if durationInMillis < 1000 ⇒ durationInMillis millis
      case durationInMillis if durationInMillis < 60000 ⇒ (durationInMillis millis).toSeconds seconds
      case durationInMillis if durationInMillis < 3600000 ⇒ (durationInMillis millis).toMinutes minutes
      case _ ⇒ (durationInMillis millis).toHours hours
    }
  }

  def getConfiguredWindows(metricType: String): Seq[FiniteDuration] = {
    metricType match {
      case MetricType.Timer   ⇒ Histogram.ConfiguredWindows.toSeq
      case MetricType.Counter ⇒ Counter.ConfiguredWindows.toSeq
      case _                  ⇒ throw new UnsupportedOperationException(s"Unknown metric type $metricType")
    }
  }

}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  def apply() = super.apply(ActorSystemSupport.system)

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) = new Settings(system.settings.config, system)
}