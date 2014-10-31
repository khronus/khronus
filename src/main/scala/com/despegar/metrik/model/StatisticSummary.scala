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

package com.despegar.metrik.model

import java.lang.reflect.Method

import scala.collection.concurrent.TrieMap

case class StatisticSummary(timestamp: Long, p50: Long, p80: Long, p90: Long, p95: Long, p99: Long, p999: Long, min: Long, max: Long, count: Long, avg: Long) extends Summary {
  override def getTimestamp = timestamp
}

object StatisticSummary {
  implicit class PimpedStatisticSummary(summary: StatisticSummary) {
    private val cache = TrieMap.empty[String, Method]

    def get(name: String): Long = {
      val method = cache.getOrElseUpdate(name, summary.getClass.getDeclaredMethod(name))
      method.invoke(summary).asInstanceOf[Long]
    }
  }
}

object Functions {
  sealed trait Function {
    def value: String
  }

  case object Count extends Functions.Function { val value = "count" }
  case object Min extends Functions.Function { val value = "min" }
  case object Max extends Functions.Function { val value = "max" }
  case object Avg extends Functions.Function { val value = "avg" }
  case object Percentile50 extends Functions.Function { val value = "p50" }
  case object Percentile80 extends Functions.Function { val value = "p80" }
  case object Percentile90 extends Functions.Function { val value = "p90" }
  case object Percentile95 extends Functions.Function { val value = "p95" }
  case object Percentile99 extends Functions.Function { val value = "p99" }
  case object Percentile999 extends Functions.Function { val value = "p999" }

  val allValues: Seq[Function] = Seq(Count, Min, Max, Avg, Percentile50, Percentile80, Percentile90, Percentile95, Percentile99, Percentile999)
  val allValuesAsString: Seq[String] = allValues.map(_.value)

  def withName(s: String): Function = allValues.find(_.toString == s).get

  implicit def influxFunctions2Value(function: Functions.Function) = function.value
}