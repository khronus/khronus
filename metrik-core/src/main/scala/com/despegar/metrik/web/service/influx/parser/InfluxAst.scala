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

case class InfluxCriteria(projections: Seq[Projection],
  table: Table,
  filters: List[Filter],
  groupBy: GroupBy,
  limit: Option[Int],
  orderAsc: Boolean = true)

// SELECT
sealed trait Projection

case class Field(name: String, alias: Option[String]) extends Projection
case class AllField() extends Projection

case class Identifier(value: String)

// FROM
case class Table(name: String, alias: Option[String])

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