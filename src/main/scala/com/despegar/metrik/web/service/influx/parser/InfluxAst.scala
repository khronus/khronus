package com.despegar.metrik.web.service.influx.parser

import scala.concurrent.duration.FiniteDuration

trait Node {
  def toString: String
}

case class InfluxCriteria(projection: Projection,
    table: Table,
    filter: Option[Expression],
    groupBy: Option[GroupBy],
    limit: Option[Int]) extends Node {

  override def toString = Seq(Some(s"select [${projection.toString}] from [${table.toString}]"),
    filter.map(x ⇒ "where " + x.toString),
    groupBy.map(_.toString),
    limit.map(x ⇒ "limit " + x.toString)).flatten.mkString(" ")
}

sealed trait Projection extends Node
case class Field(name: String, alias: Option[String]) extends Projection {
  override def toString = s"$name as $alias"
}
case class AllField() extends Projection {
  override def toString = "*"
}

case class Table(name: String, alias: Option[String]) extends Node {
  override def toString = s"$name $alias"

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

trait Expression extends Node

trait ProjectionExpression extends Expression {
  def function: String
}
case class Count(name: String) extends ProjectionExpression {
  override def toString = s"count($name)"
  override def function = Functions.Count
}
case class Avg(name: String) extends ProjectionExpression {
  override def toString = s"avg($name)"
  override def function = Functions.Avg
}
case class Min(name: String) extends ProjectionExpression {
  override def toString = s"min($name)"
  override def function = Functions.Min
}
case class Max(name: String) extends ProjectionExpression {
  override def toString = s"max($name)"
  override def function = Functions.Max
}

case class Percentile50(name: String) extends ProjectionExpression {
  override def toString = s"p50($name)"
  override def function = Functions.Percentile50
}
case class Percentile80(name: String) extends ProjectionExpression {
  override def toString = s"p80($name)"
  override def function = Functions.Percentile80
}
case class Percentile90(name: String) extends ProjectionExpression {
  override def toString = s"p90($name)"
  override def function = Functions.Percentile90
}
case class Percentile95(name: String) extends ProjectionExpression {
  override def toString = s"p95($name)"
  override def function = Functions.Percentile95
}
case class Percentile99(name: String) extends ProjectionExpression {
  override def toString = s"p99($name)"
  override def function = Functions.Percentile99
}
case class Percentile999(name: String) extends ProjectionExpression {
  override def toString = s"p999($name)"
  override def function = Functions.Percentile999
}

trait BinaryOperation extends Expression {
  val leftExpression: Expression
  val rightExpression: Expression

  val operator: String

  override def toString = s"(${leftExpression.toString}) $operator (${rightExpression.toString})"
}

object Operators {
  val Or = "or"
  val And = "and"
  val Eq = "="
  val Neq = "<>"
  val Gte = ">="
  val Gt = ">"
  val Lte = "<="
  val Lt = "<"
}

case class Or(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Or
}
case class And(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.And
}
case class Eq(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Eq
}
case class Neq(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Neq
}
case class Ge(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Gte
}
case class Gt(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Gt
}
case class Le(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Lte
}
case class Lt(leftExpression: Expression, rightExpression: Expression) extends BinaryOperation {
  val operator = Operators.Lt
}

trait LiteralExpression extends Expression {
  def gatherFields = Seq.empty
}
case class IntLiteral(value: Long) extends LiteralExpression {
  override def toString = value.toString
}
case class Identifier(value: String) extends LiteralExpression {
  override def toString = value.toString
}
case class StringLiteral(value: String) extends LiteralExpression {
  override def toString = "\"" + value.toString + "\"" // TODO: escape...
}
case class NullLiteral() extends LiteralExpression {
  override def toString = "null"
}
case class DateLiteral(dateStr: String) extends LiteralExpression {
  override def toString = s"date [$dateStr]"
}

case class GroupBy(duration: FiniteDuration) extends Node {
  override def toString = s"group by $duration"
}

object TimeSuffixes {
  val Seconds = "s"
  val Minutes = "m"
  val Hours = "h"
  val Days = "d"
  val Weeks = "w"
}