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

  def toSqlString =
    Seq(Some("select"),
      Some(projection.toSqlString),
      Some("from " + table.toSqlString),
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

case class ExpressionProjection(expr: Expression, alias: Option[String]) extends Projection {
  def toSqlString = Seq(Some(expr.toSqlString), alias).flatten.mkString(" as ")
}
case class StarProjection() extends Projection {
  def toSqlString = "*"
}

trait Expression extends Node {
  def isLiteral: Boolean = false

  // (col, true if aggregate context false otherwise)
  // only gathers fields within this context (
  // wont traverse into subselects )
  def gatherFields: Seq[(FieldIdentifier, Boolean)]
}

trait BinaryOperation extends Expression {
  val lhs: Expression
  val rhs: Expression

  val operator: String

  override def isLiteral = lhs.isLiteral && rhs.isLiteral
  def gatherFields = lhs.gatherFields ++ rhs.gatherFields

  def toSqlString = Seq("(" + lhs.toSqlString + ")", operator, "(" + rhs.toSqlString + ")") mkString " "
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
  def toSqlString = Seq(opStr, "(", expr.toSqlString, ")") mkString " "
}

case class Not(expr: Expression) extends UnaryOperation {
  val opStr = "not"
}

case class FieldIdentifier(qualifier: Option[String], name: String, symbol: Symbol = null) extends Expression {
  def gatherFields = Seq((this, false))
  def toSqlString = Seq(qualifier, Some(name)).flatten.mkString(".")
}

trait SqlAgg extends Expression
case class CountExpr(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = Seq(Some("count("), Some(expr.toSqlString), Some(")")).flatten.mkString("")
}
case class Avg(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = Seq(Some("avg("), Some(expr.toSqlString), Some(")")).flatten.mkString("")
}
case class Min(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = "min(" + expr.toSqlString + ")"
}
case class Max(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = "max(" + expr.toSqlString + ")"
}
case class AggCall(name: String, args: Seq[Expression]) extends SqlAgg {
  def gatherFields = args.flatMap(_.gatherFields)
  def toSqlString = Seq(name, "(", args.map(_.toSqlString).mkString(", "), ")").mkString("")
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
case class StringLiteral(v: String) extends LiteralExpr {
  def toSqlString = "\"" + v.toString + "\"" // TODO: escape...
}
case class NullLiteral() extends LiteralExpr {
  def toSqlString = "null"
}
case class DateLiteral(d: String) extends LiteralExpr {
  def toSqlString = Seq("date", "\"" + d + "\"") mkString " "
}

case class Table(name: String, alias: Option[String]) extends Node {
  def toSqlString = Seq(Some(name), alias).flatten.mkString(" ")

}

case class GroupBy(keys: Seq[Expression]) extends Node {
  def toSqlString = Seq(Some("group by"), Some(keys.map(_.toSqlString).mkString(", "))).flatten.mkString(" ")
}
