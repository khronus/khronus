package com.despegar.metrik.web.service.influx.parser

import com.sun.corba.se.spi.legacy.interceptor.UnknownType

trait Node {
  def toSqlString: String
}

case class InfluxCriteria(projection: Projection,
    table: Table,
    filter: Option[Expression],
    groupBy: Option[GroupBy],
    limit: Option[Int]) extends Node {

  def toSqlString = Seq(Some(s"select [${projection.toSqlString}] from [${table.toSqlString}]"),
    filter.map(x ⇒ "where " + x.toSqlString),
    groupBy.map(_.toSqlString),
    limit.map(x ⇒ "limit " + x.toString)).flatten.mkString(" ")
}

sealed trait Projection extends Node
case class Field(name: String, alias: Option[String]) extends Projection {
  def toSqlString = s"$name as $alias"
}
case class AllField() extends Projection {
  override def toSqlString = "*"
}

object InternalNames {
  val Count = "count"
  val Min = "min"
  val Max = "max"
  val Avg = "avg"
}

trait ProjectionExpression extends Expression
case class CountExpr(name: String) extends ProjectionExpression {
  def toSqlString = s"count($name)"
  override def gatherFields = Seq(InternalNames.Count)
}
case class Avg(name: String) extends ProjectionExpression {
  def toSqlString = s"avg($name)"
  override def gatherFields = Seq(InternalNames.Avg)
}
case class Min(name: String) extends ProjectionExpression {
  def toSqlString = s"min($name)"
  override def gatherFields = Seq(InternalNames.Min)
}
case class Max(name: String) extends ProjectionExpression {
  def toSqlString = s"max($name)"
  override def gatherFields = Seq(InternalNames.Max)
}

trait Expression extends Node {
  def isLiteral: Boolean = false

  def gatherFields: Seq[String]
}

trait BinaryOperation extends Expression {
  val lhs: Expression
  val rhs: Expression

  val operator: String

  override def isLiteral = lhs.isLiteral && rhs.isLiteral
  def gatherFields = lhs.gatherFields ++ rhs.gatherFields

  def toSqlString = s"(${lhs.toSqlString}) $operator (${rhs.toSqlString})"
}

case class Or(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "or"
}
case class And(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "and"
}
case class Eq(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "="
}
case class Neq(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "<>"
}
case class Ge(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "<="
}
case class Gt(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = "<"
}
case class Le(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = ">="
}
case class Lt(lhs: Expression, rhs: Expression) extends BinaryOperation {
  val operator = ">"
}
case class Like(lhs: Expression, rhs: Expression, negate: Boolean) extends BinaryOperation {
  val operator = if (negate) "not like" else "like"
}

trait UnaryOperation extends Expression {
  val expr: Expression
  val opStr: String
  override def isLiteral = expr.isLiteral
  def gatherFields = expr.gatherFields
  def toSqlString = s"$opStr (${expr.toSqlString})"
}

case class Not(expr: Expression) extends UnaryOperation {
  val opStr = "not"
}

trait LiteralExpr extends Expression {
  override def isLiteral = true
  def gatherFields = Seq.empty
}
case class IntLiteral(v: Long) extends LiteralExpr {
  def toSqlString = v.toString
}
case class FloatLiteral(v: Double) extends LiteralExpr {
  def toSqlString = v.toString
}
case class Identifier(v: String) extends LiteralExpr {
  def toSqlString = v.toString
}
case class StringLiteral(v: String) extends LiteralExpr {
  def toSqlString = "\"" + v.toString + "\"" // TODO: escape...
}
case class NullLiteral() extends LiteralExpr {
  def toSqlString = "null"
}
case class DateLiteral(d: String) extends LiteralExpr {
  def toSqlString = s"date [$d]"
}

case class Table(name: String, alias: Option[String]) extends Node {
  def toSqlString = s"$name $alias"

}

case class GroupBy(keys: Seq[Expression]) extends Node {
  def toSqlString = Seq(Some("group by"), Some(keys.map(_.toSqlString).mkString(", "))).flatten.mkString(" ")
}
