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

package com.searchlight.khronus.model

import scala.concurrent.duration._

case class HistogramSummary(timestamp: Timestamp, p50: Long, p80: Long, p90: Long, p95: Long, p99: Long, p999: Long, min: Long, max: Long, count: Long, mean: Long) extends Summary {
  override def toString = s"HistogramSummary(timestamp=${timestamp.ms},count=$count,...)"
}

object Functions {

  sealed trait Function {
    def name: String

    def apply(summary: Summary): Double = {
      summary.get(name).toDouble
    }
  }

  sealed trait MetaFunction extends Function {
    def underlyingFunction: Function
    def apply(summary: Summary, timeWindowInMillis: Long): Double
  }

  sealed trait Percentile extends Function {
    def name: String

    def value: Int
  }

  case object Count extends Functions.Function {
    val name = "count"
  }

  case object Min extends Functions.Function {
    val name = "min"
  }

  case object Max extends Functions.Function {
    val name = "max"
  }

  case object Mean extends Functions.Function {
    val name = "mean"
  }

  case object Percentile50 extends Functions.Percentile {
    val name = "p50";
    val value = 50
  }

  case object Percentile80 extends Functions.Percentile {
    val name = "p80";
    val value = 80
  }

  case object Percentile90 extends Functions.Percentile {
    val name = "p90";
    val value = 90
  }

  case object Percentile95 extends Functions.Percentile {
    val name = "p95";
    val value = 95
  }

  case object Percentile99 extends Functions.Percentile {
    val name = "p99";
    val value = 99
  }

  case object Percentile999 extends Functions.Percentile {
    val name = "p999";
    val value = 999
  }

  case object Cpm extends Functions.MetaFunction {
    val name = "cpm"
    val underlyingFunction = Count

    override def apply(summary: Summary, timeWindowInMillis: Long): Double = {
      val count = summary.get(underlyingFunction.name)
      val minutesOnWindow = timeWindowInMillis.toDouble / (1 minutes).toMillis
      count / minutesOnWindow
    }
  }

  val allPercentiles: Seq[Percentile] = Seq(Percentile50, Percentile80, Percentile90, Percentile95, Percentile99, Percentile999)
  val allPercentileNames: Seq[String] = allPercentiles.map(_.name)
  val allPercentilesValues: Seq[Int] = allPercentiles.map(_.value)

  def percentileByValue(i: Int): Function = allPercentiles.find(_.value == i).get

  val all: Seq[Function] = allPercentiles ++ Seq(Count, Min, Max, Mean, Cpm)
  val allNames: Seq[String] = all.map(_.name)

  val allHistogramFunctions: Seq[String] = allNames
  val allCounterFunctions: Seq[String] = Seq(Count.name, Cpm.name)

  def withName(s: String): Function = all.find(_.name == s).get

  implicit def influxFunctions2Value(function: Functions.Function) = function.name
}