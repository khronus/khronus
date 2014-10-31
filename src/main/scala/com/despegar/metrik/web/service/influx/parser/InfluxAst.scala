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

package com.despegar.metrik.web.service.influx.parser

import scala.concurrent.duration.FiniteDuration

case class InfluxCriteria(projection: Projection,
  table: Table,
  filters: List[Filter],
  groupBy: GroupBy,
  limit: Option[Int])

sealed trait Projection

case class Field(name: String, alias: Option[String]) extends Projection
case class AllField() extends Projection

case class Identifier(value: String) extends Expression

case class Table(name: String, alias: Option[String])

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

trait Expression

trait ProjectionExpression extends Expression {
  def function: String
}
case class Count(name: String) extends ProjectionExpression {
  override def function = Functions.Count
}
case class Avg(name: String) extends ProjectionExpression {
  override def function = Functions.Avg
}
case class Min(name: String) extends ProjectionExpression {
  override def function = Functions.Min
}
case class Max(name: String) extends ProjectionExpression {
  override def function = Functions.Max
}
case class Percentile50(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile50
}
case class Percentile80(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile80
}
case class Percentile90(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile90
}
case class Percentile95(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile95
}
case class Percentile99(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile99
}
case class Percentile999(name: String) extends ProjectionExpression {
  override def function = Functions.Percentile999
}

trait Filter
case class TimeFilter(identifier: String = "time", operator: String, value: Long) extends Filter
case class StringFilter(identifier: String, operator: String, value: String) extends Filter

object Operators {
  val And = "and"
  val Eq = "="
  val Neq = "<>"
  val Gte = ">="
  val Gt = ">"
  val Lte = "<="
  val Lt = "<"
}

case class GroupBy(duration: FiniteDuration) {
  override def toString = s"group by $duration"
}

object TimeSuffixes {
  val Seconds = "s"
  val Minutes = "m"
  val Hours = "h"
  val Days = "d"
  val Weeks = "w"

}