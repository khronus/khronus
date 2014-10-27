package com.despegar.metrik.web.service.influx.parser

import com.sun.corba.se.spi.legacy.interceptor.UnknownType

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


// TODO - Add percentiles
object Functions {
  val Count = "count"
  val Min = "min"
  val Max = "max"
  val Avg = "avg"
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


case class GroupBy(keys: Seq[Expression]) extends Node {
  override def toString = Seq(Some("group by"), Some(keys.map(_.toString).mkString(", "))).flatten.mkString(" ")
}
