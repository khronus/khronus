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

import com.despegar.metrik.model.Functions
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