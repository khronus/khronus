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

package com.searchlight.khronus.influx.parser

import scala.concurrent.duration.FiniteDuration
import com.searchlight.khronus.model.Metric
import com.searchlight.khronus.influx.parser.MathOperators.MathOperator
import scala.math._

case class InfluxCriteria(projections: Seq[SimpleProjection],
  sources: Seq[Source],
  filters: Seq[Filter],
  groupBy: GroupBy,
  fillValue: Option[Double] = None,
  scale: Option[Double] = None,
  limit: Int = Int.MaxValue,
  orderAsc: Boolean = true)

// SELECT
sealed trait Projection
sealed trait SimpleProjection extends Projection {
  def seriesId: String
}
trait AliasingTable {
  def tableId: Option[String]
}

case class AllField(tableId: Option[String]) extends Projection with AliasingTable

case class Field(name: String, alias: Option[String], tableId: Option[String]) extends SimpleProjection with AliasingTable {
  override def seriesId = s"${tableId.getOrElse("")}.${alias.getOrElse(name)}"
}
case class Number(value: Double, alias: Option[String] = None) extends SimpleProjection {
  override def seriesId = alias.getOrElse("")
}
case class Operation(left: SimpleProjection, right: SimpleProjection, operator: MathOperator, alias: String) extends SimpleProjection {
  override def seriesId = alias
}

object MathOperators {

  trait MathOperator {
    def symbol: String

    def apply(firstOperand: Double, secondOperand: Double): Double
  }

  case object Plus extends MathOperator {
    val symbol = "+"

    def apply(firstOperand: Double, secondOperand: Double): Double = {
      firstOperand + secondOperand
    }
  }

  case object Minus extends MathOperator {
    val symbol = "-"

    def apply(firstOperand: Double, secondOperand: Double): Double = {
      firstOperand - secondOperand
    }
  }

  case object Multiply extends MathOperator {
    val symbol = "*"

    def apply(firstOperand: Double, secondOperand: Double): Double = {
      firstOperand * secondOperand
    }
  }

  case object Divide extends MathOperator {
    val symbol = "/"

    def apply(firstOperand: Double, secondOperand: Double): Double = {
      if (secondOperand == 0l) {
        throw new UnsupportedOperationException("Could not divide by zero")
      }
      firstOperand / secondOperand
    }
  }

  def allSymbols: Seq[String] = Seq(Plus.symbol, Minus.symbol, Multiply.symbol, Divide.symbol)

  def getBySymbol(symbol: String) = symbol match {
    case Plus.symbol     ⇒ Plus
    case Minus.symbol    ⇒ Minus
    case Multiply.symbol ⇒ Multiply
    case Divide.symbol   ⇒ Divide
    case _               ⇒ throw new IllegalArgumentException(s"Unknown operator $symbol")
  }

}

case class Identifier(value: String)

// FROM
case class Table(name: String, alias: Option[String])
case class Source(metric: Metric, alias: Option[String] = None)

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
  val Milliseconds = "ms"
  val Seconds = "s"
  val Minutes = "m"
  val Hours = "h"
  val Days = "d"
  val Weeks = "w"

}