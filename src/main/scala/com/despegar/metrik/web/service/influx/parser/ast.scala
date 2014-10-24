package com.despegar.metrik.web.service.influx.parser

trait Node {
  def toSqlString: String
}

case class InfluxCriteria(projections: Seq[Projection],
    table: Table,
    filter: Option[Expression],
    groupBy: Option[GroupBy],
    limit: Option[Int]) extends Node {
  def toSqlString =
    Seq(Some("select"),
      Some(projections.map(_.toSqlString).mkString(", ")),
      Some("from " + table.toSqlString),
      filter.map(x ⇒ "where " + x.toSqlString),
      groupBy.map(_.toSqlString),
      limit.map(x ⇒ "limit " + x.toString)).flatten.mkString(" ")
}

trait Projection extends Node
case class ExpressionProjection(expr: Expression, alias: Option[String]) extends Projection {
  def toSqlString = Seq(Some(expr.toSqlString), alias).flatten.mkString(" as ")
}
case class StarProjection() extends Projection {
  def toSqlString = "*"
}

trait Expression extends Node {
  def getType: DataType = UnknownType
  def isLiteral: Boolean = false

  // is the r-value of this expression a literal?
  def isRValueLiteral: Boolean = isLiteral

  // (col, true if aggregate context false otherwise)
  // only gathers fields within this context (
  // wont traverse into subselects )
  def gatherFields: Seq[(FieldIdent, Boolean)]
}

trait Binop extends Expression {
  val lhs: Expression
  val rhs: Expression

  val opStr: String

  override def isLiteral = lhs.isLiteral && rhs.isLiteral
  def gatherFields = lhs.gatherFields ++ rhs.gatherFields

  def copyWithChildren(lhs: Expression, rhs: Expression): Binop

  def toSqlString = Seq("(" + lhs.toSqlString + ")", opStr, "(" + rhs.toSqlString + ")") mkString " "
}

case class Or(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "or"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class And(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "and"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

trait EqualityLike extends Binop
case class Eq(lhs: Expression, rhs: Expression) extends EqualityLike {
  val opStr = "="
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Neq(lhs: Expression, rhs: Expression) extends EqualityLike {
  val opStr = "<>"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

trait InequalityLike extends Binop
case class Ge(lhs: Expression, rhs: Expression) extends InequalityLike {
  val opStr = "<="
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Gt(lhs: Expression, rhs: Expression) extends InequalityLike {
  val opStr = "<"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Le(lhs: Expression, rhs: Expression) extends InequalityLike {
  val opStr = ">="
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Lt(lhs: Expression, rhs: Expression) extends InequalityLike {
  val opStr = ">"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

case class Like(lhs: Expression, rhs: Expression, negate: Boolean) extends Binop {
  val opStr = if (negate) "not like" else "like"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

case class Plus(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "+"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Minus(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "-"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

case class Mult(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "*"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}
case class Div(lhs: Expression, rhs: Expression) extends Binop {
  val opStr = "/"
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs)
}

trait Unop extends Expression {
  val expr: Expression
  val opStr: String
  override def isLiteral = expr.isLiteral
  def gatherFields = expr.gatherFields
  def toSqlString = Seq(opStr, "(", expr.toSqlString, ")") mkString " "
}

case class Not(expr: Expression) extends Unop {
  val opStr = "not"
}

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null) extends Expression {
  def gatherFields = Seq((this, false))
  def toSqlString = Seq(qualifier, Some(name)).flatten.mkString(".")
}

trait SqlAgg extends Expression
case class CountStar() extends SqlAgg {
  def gatherFields = Seq.empty
  def toSqlString = "count(*)"
}
case class CountExpr(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = Seq(Some("count("), Some(expr.toSqlString), Some(")")).flatten.mkString("")
}
case class Sum(expr: Expression) extends SqlAgg {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def toSqlString = Seq(Some("sum("), Some(expr.toSqlString), Some(")")).flatten.mkString("")
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

trait SqlFunction extends Expression {
  val name: String
  val args: Seq[Expression]
  override def isLiteral = args.foldLeft(true)(_ && _.isLiteral)
  def gatherFields = args.flatMap(_.gatherFields)
  def toSqlString = Seq(name, "(", args.map(_.toSqlString) mkString ", ", ")") mkString ""
}

case class FunctionCall(name: String, args: Seq[Expression]) extends SqlFunction

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType


case class UnaryPlus(expr: Expression) extends Unop {
  val opStr = "+"
}
case class UnaryMinus(expr: Expression) extends Unop {
  val opStr = "-"
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
case class IntervalLiteral(e: String, unit: ExtractType) extends LiteralExpr {
  def toSqlString = Seq("interval", "\"" + e + "\"", unit.toString) mkString " "
}

case class Table(name: String, alias: Option[String]) extends Node {
  def toSqlString = Seq(Some(name), alias).flatten.mkString(" ")

}

case class GroupBy(keys: Seq[Expression]) extends Node {
  def toSqlString = Seq(Some("group by"), Some(keys.map(_.toSqlString).mkString(", "))).flatten.mkString(" ")
}
