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

case class StatisticSummary(timestamp: Timestamp, p50: Long, p80: Long, p90: Long, p95: Long, p99: Long, p999: Long, min: Long, max: Long, count: Long, avg: Long) extends Summary {
  override def toString = s"StatisticSummary(timestamp=${timestamp.ms},count=$count,...)"
}

object Functions {
  sealed trait Function {
    def name: String
  }

  sealed trait Percentile extends Function {
    def name: String
    def value: Int
  }

  case object Count extends Functions.Function { val name = "count" }
  case object Min extends Functions.Function { val name = "min" }
  case object Max extends Functions.Function { val name = "max" }
  case object Avg extends Functions.Function { val name = "avg" }
  case object Percentile50 extends Functions.Percentile { val name = "p50"; val value = 50 }
  case object Percentile80 extends Functions.Percentile { val name = "p80"; val value = 80 }
  case object Percentile90 extends Functions.Percentile { val name = "p90"; val value = 90 }
  case object Percentile95 extends Functions.Percentile { val name = "p95"; val value = 95 }
  case object Percentile99 extends Functions.Percentile { val name = "p99"; val value = 99 }
  case object Percentile999 extends Functions.Percentile { val name = "p999"; val value = 999 }

  val allPercentiles: Seq[Percentile] = Seq(Percentile50, Percentile80, Percentile90, Percentile95, Percentile99, Percentile999)
  val allPercentileNames: Seq[String] = allPercentiles.map(_.name)
  val allPercentilesValues: Seq[Int] = allPercentiles.map(_.value)
  def percentileByValue(i: Int): Function = allPercentiles.find(_.value == i).get

  val all: Seq[Function] = allPercentiles ++ Seq(Count, Min, Max, Avg)
  val allNames: Seq[String] = all.map(_.name)

  val allHistogramFunctions: Seq[String] = allNames
  val allCounterFunctions: Seq[String] = Seq(Count.name)

  def withName(s: String): Function = all.find(_.name == s).get

  implicit def influxFunctions2Value(function: Functions.Function) = function.name
}