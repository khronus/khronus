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

package com.searchlight.khronus.util

import java.util.Map.Entry

import com.searchlight.khronus.model.MetricSpecs.{Dimensional, MetricSpec, NonDimensional}
import com.searchlight.khronus.model._
import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}

object Settings {

  val config = ConfigFactory.load()

  object Master {
    val TickCronExpression = config.getString("khronus.master.tick-expression")
    val CheckLeaderCronExpression = config.getString("khronus.master.check-leader-expression")
    val DiscoveryStartDelay = FiniteDuration(config.getDuration("khronus.master.discovery-start-delay", MILLISECONDS), MILLISECONDS)
    val DiscoveryInterval = FiniteDuration(config.getDuration("khronus.master.discovery-interval", MILLISECONDS), MILLISECONDS)
    val WorkerBatchSize = config.getInt("khronus.master.worker-batch-size")
    val MaxDelayBetweenClocks = FiniteDuration(config.getDuration("khronus.master.max-delay-between-clocks", MILLISECONDS), MILLISECONDS)
  }

  object Http {
    val Interface = config.getString("khronus.endpoint")
    val Port: Int = config.getInt("khronus.port")
  }

  object Window {
    val TickDelay = config.getInt("khronus.windows.tick-delay")
    val ConfiguredWindowsForNonDimensional = config.getDurationList("khronus.windows.durations.nonDimensionalMetrics", MILLISECONDS).asScala.map(adjustDuration(_))
    val ConfiguredWindowsForDimensional = config.getDurationList("khronus.windows.durations.dimensionalMetrics", MILLISECONDS).asScala.map(adjustDuration(_))
    val RawDuration = 1 millis
    val WindowDurationsForNonDimensional = RawDuration +: ConfiguredWindowsForNonDimensional
    val WindowDurationsForDimensional = RawDuration +: ConfiguredWindowsForDimensional
  }

  object Dashboard {
    val MinResolutionPoints: Int = config.getInt("khronus.dashboards.min-resolution-points")
    val MaxResolutionPoints: Int = config.getInt("khronus.dashboards.max-resolution-points")
  }

  object InternalMetrics {
    val Enabled: Boolean = config.getBoolean("khronus.internal-metrics.enabled")
    val CheckOutliers: Boolean = config.getBoolean("khronus.internal-metrics.check-outliers")
  }

  object CassandraCluster {
    private val cassandraCfg = config.getConfig("khronus.cassandra.cluster")
    val ClusterName = cassandraCfg.getString("name")
    val MaxConnectionsPerHost = cassandraCfg.getInt("max-connections-per-host")
    val SocketTimeout = cassandraCfg.getDuration("socket-timeout", MILLISECONDS).toInt
    val ConnectionTimeout = cassandraCfg.getDuration("connection-timeout", MILLISECONDS).toInt
    val Port = cassandraCfg.getInt("port")
    val Seeds = cassandraCfg.getString("seeds").split(",").toSeq
    val KeyspaceNameSuffix = cassandraCfg.getString("keyspace-name-suffix")
  }

  object CassandraMeta {
    private val cassandraCfg = config.getConfig("khronus.cassandra.meta")
    val ReplicationFactor = cassandraCfg.getInt("rf")
    val insertChunkSize = cassandraCfg.getInt("insert-chunk-size")
  }

  object CassandraBuckets {
    private val cassandraCfg = config.getConfig("khronus.cassandra.buckets")
    val ReplicationFactor = cassandraCfg.getInt("rf")
    val insertChunkSize = cassandraCfg.getInt("insert-chunk-size")
  }

  object CassandraSummaries {
    private val cassandraCfg = config.getConfig("khronus.cassandra.summaries")
    val ReplicationFactor = cassandraCfg.getInt("rf")
    val insertChunkSize = cassandraCfg.getInt("insert-chunk-size")
  }

  object CassandraLeaderElection {
    private val cassandraCfg = config.getConfig("khronus.cassandra.leader-election")
    val ReplicationFactor = cassandraCfg.getInt("rf")
  }

  object Histograms {
    private val histogramConfig = config.getConfig("khronus.histogram")
    val BucketRetentionPolicy = histogramConfig.getDuration("bucket-retention-policy", SECONDS).toInt
    val TimeWindows: Map[MetricSpec, Seq[Window]] = Map(NonDimensional -> Window.WindowDurationsForNonDimensional.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      HistogramTimeWindow(duration, previous, Window.WindowDurationsForNonDimensional.last != duration)
    }.toSeq, Dimensional -> Window.WindowDurationsForDimensional.sliding(2).map { dp ⇒
      HistogramTimeWindow(dp.last, dp.head, true)
    }.toSeq)

    val SummaryRetentionPolicyDefault = Duration(histogramConfig.getString("summary-retention-policy.default"))
    val SummaryRetentionPolicyOverrides: Map[Duration, Duration] = {
      val list: Iterable[ConfigObject] = histogramConfig.getObjectList("summary-retention-policy.overrides").asScala
      (for {
        item: ConfigObject ← list
        entry: Entry[String, ConfigValue] ← item.entrySet().asScala
        resolution = Duration(entry.getKey)
        ttl = Duration(entry.getValue.unwrapped().toString)
      } yield (resolution, ttl)).toMap
    }

    val SummaryRetentionPolicies = getRetentionPolicies(SummaryRetentionPolicyDefault, SummaryRetentionPolicyOverrides, Window.WindowDurationsForNonDimensional)

    val BucketLimit = histogramConfig.getInt("bucket-limit")
    val BucketFetchSize = histogramConfig.getInt("bucket-fetch-size")
    val SummaryLimit = histogramConfig.getInt("summary-limit")
    val SummaryFetchSize = histogramConfig.getInt("summary-fetch-size")

  }

  object Counters {
    private val counterConfig = config.getConfig("khronus.counter")
    val BucketRetentionPolicy = counterConfig.getDuration("bucket-retention-policy", SECONDS).toInt
    val SummaryRetentionPolicyDefault = Duration(counterConfig.getString("summary-retention-policy.default"))
    val SummaryRetentionPolicyOverrides: Map[Duration, Duration] = {
      val list: Iterable[ConfigObject] = counterConfig.getObjectList("summary-retention-policy.overrides").asScala
      (for {
        item: ConfigObject ← list
        entry: Entry[String, ConfigValue] ← item.entrySet().asScala
        resolution = Duration(entry.getKey)
        ttl = Duration(entry.getValue.unwrapped().toString)
      } yield (resolution, ttl)).toMap
    }

    val TimeWindows: Map[MetricSpec, Seq[Window]] = Map(NonDimensional -> Window.WindowDurationsForNonDimensional.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      CounterTimeWindow(duration, previous, Window.WindowDurationsForNonDimensional.last != duration)
    }.toSeq, Dimensional -> Window.WindowDurationsForDimensional.sliding(2).map { dp ⇒
      CounterTimeWindow(dp.last, dp.head, true)
    }.toSeq)

    val SummaryRetentionPolicies = getRetentionPolicies(SummaryRetentionPolicyDefault, SummaryRetentionPolicyOverrides, Window.WindowDurationsForNonDimensional)

    val BucketLimit = counterConfig.getInt("bucket-limit")
    val BucketFetchSize = counterConfig.getInt("bucket-fetch-size")
    val SummaryLimit = counterConfig.getInt("summary-limit")
    val SummaryFetchSize = counterConfig.getInt("summary-fetch-size")
  }

  object Gauges {
    private val gaugeConfig = config.getConfig("khronus.gauge")
    val BucketRetentionPolicy = gaugeConfig.getDuration("bucket-retention-policy", SECONDS).toInt
    val SummaryRetentionPolicyDefault = Duration(gaugeConfig.getString("summary-retention-policy.default"))
    val SummaryRetentionPolicyOverrides: Map[Duration, Duration] = {
      val list: Iterable[ConfigObject] = gaugeConfig.getObjectList("summary-retention-policy.overrides").asScala
      (for {
        item: ConfigObject ← list
        entry: Entry[String, ConfigValue] ← item.entrySet().asScala
        resolution = Duration(entry.getKey)
        ttl = Duration(entry.getValue.unwrapped().toString)
      } yield (resolution, ttl)).toMap
    }

    val TimeWindows: Map[MetricSpec, Seq[Window]] = Map(NonDimensional -> Window.WindowDurationsForNonDimensional.sliding(2).map { dp ⇒
      val previous = dp.head
      val duration = dp.last
      GaugeTimeWindow(duration, previous, Window.WindowDurationsForNonDimensional.last != duration)
    }.toSeq, Dimensional -> Window.WindowDurationsForDimensional.sliding(2).map { dp ⇒
      GaugeTimeWindow(dp.last, dp.head, true)
    }.toSeq)

    val SummaryRetentionPolicies = getRetentionPolicies(SummaryRetentionPolicyDefault, SummaryRetentionPolicyOverrides, Window.WindowDurationsForNonDimensional)

    val BucketLimit = gaugeConfig.getInt("bucket-limit")
    val BucketFetchSize = gaugeConfig.getInt("bucket-fetch-size")
    val SummaryLimit = gaugeConfig.getInt("summary-limit")
    val SummaryFetchSize = gaugeConfig.getInt("summary-fetch-size")
  }

  object BucketCache {
    private val bucketCacheConfig = config.getConfig("khronus.bucket-cache")

    val Enabled = bucketCacheConfig.getBoolean("enabled")

    val MaxStore = bucketCacheConfig.getInt("max-store")

    val MaxMetrics: Map[MetricType, Int] = Map(Histogram -> bucketCacheConfig.getInt(s"max-metrics.$Histogram"),
      Counter -> bucketCacheConfig.getInt(s"max-metrics.$Counter"), Gauge -> bucketCacheConfig.getInt(s"max-metrics.$Gauge"))

    def IsEnabledFor(metricType: MetricType): Boolean = Option(bucketCacheConfig.getBoolean(metricType.toString)).getOrElse(false)
  }

  private def adjustDuration(durationMS: Long): FiniteDuration = {
    durationMS match {
      case durationInMillis if durationInMillis < 1000 ⇒ durationInMillis millis
      case durationInMillis if durationInMillis < 60000 ⇒ (durationInMillis millis).toSeconds seconds
      case durationInMillis if durationInMillis < 3600000 ⇒ (durationInMillis millis).toMinutes minutes
      case _ ⇒ (durationMS millis).toHours hours
    }
  }

  private def getRetentionPolicies(default: Duration, overrides: Map[Duration, Duration], windows: Seq[Duration]): Map[Duration, Duration] = {
    windows.map(window ⇒ {
      if (overrides.contains(window)) window -> overrides(window) else window -> default
    }).toMap
  }

}

