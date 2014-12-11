/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.influx.parser

import scala.concurrent.duration.FiniteDuration
import com.despegar.khronus.model.Metric

case class InfluxCriteria(projections: Seq[Field],
  tables: Seq[Metric],
  filters: List[Filter],
  groupBy: GroupBy,
  limit: Int = Int.MaxValue,
  orderAsc: Boolean = true)

// SELECT
sealed trait Projection

case class Field(name: String, alias: Option[String]) extends Projection
case class AllField() extends Projection

case class Identifier(value: String)

// WHERE
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

// GROUP BY
case class GroupBy(forceResolution: Boolean = false, duration: FiniteDuration) {
  override def toString = s"group by $duration"
}

object TimeSuffixes {
  val Seconds = "s"
  val Minutes = "m"
  val Hours = "h"
  val Days = "d"
  val Weeks = "w"

}