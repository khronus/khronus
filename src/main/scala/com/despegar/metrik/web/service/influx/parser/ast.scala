package com.despegar.metrik.web.service.influx.parser

trait Node extends PrettyPrinters {
  val ctx: Context

  def copyWithContext(ctx: Context): Node

  // emit sql repr of node
  def sql: String
}

case class MetricCriteria(projections: Seq[Projection],
    table: Table,
    filter: Option[Expression],
    groupBy: Option[GroupBy],
    limit: Option[Int], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql =
    Seq(Some("select"),
      Some(projections.map(_.sql).mkString(", ")),
      Some("from " + table.sql),
      filter.map(x ⇒ "where " + x.sql),
      groupBy.map(_.sql),
      limit.map(x ⇒ "limit " + x.toString)).flatten.mkString(" ")
}

trait Projection extends Node
case class ExpressionProjection(expr: Expression, alias: Option[String], ctx: Context = null) extends Projection {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = Seq(Some(expr.sql), alias).flatten.mkString(" as ")
}
case class StarProjection(ctx: Context = null) extends Projection {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = "*"
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

  def sql = Seq("(" + lhs.sql + ")", opStr, "(" + rhs.sql + ")") mkString " "
}

case class Or(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "or"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class And(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "and"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait EqualityLike extends Binop
case class Eq(lhs: Expression, rhs: Expression, ctx: Context = null) extends EqualityLike {
  val opStr = "="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Neq(lhs: Expression, rhs: Expression, ctx: Context = null) extends EqualityLike {
  val opStr = "<>"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait InequalityLike extends Binop
case class Ge(lhs: Expression, rhs: Expression, ctx: Context = null) extends InequalityLike {
  val opStr = "<="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Gt(lhs: Expression, rhs: Expression, ctx: Context = null) extends InequalityLike {
  val opStr = "<"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Le(lhs: Expression, rhs: Expression, ctx: Context = null) extends InequalityLike {
  val opStr = ">="
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Lt(lhs: Expression, rhs: Expression, ctx: Context = null) extends InequalityLike {
  val opStr = ">"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class Like(lhs: Expression, rhs: Expression, negate: Boolean, ctx: Context = null) extends Binop {
  val opStr = if (negate) "not like" else "like"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class Plus(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "+"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Minus(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "-"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

case class Mult(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "*"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}
case class Div(lhs: Expression, rhs: Expression, ctx: Context = null) extends Binop {
  val opStr = "/"
  def copyWithContext(c: Context) = copy(ctx = c)
  def copyWithChildren(lhs: Expression, rhs: Expression) = copy(lhs = lhs, rhs = rhs, ctx = null)
}

trait Unop extends Expression {
  val expr: Expression
  val opStr: String
  override def isLiteral = expr.isLiteral
  def gatherFields = expr.gatherFields
  def sql = Seq(opStr, "(", expr.sql, ")") mkString " "
}

case class Not(expr: Expression, ctx: Context = null) extends Unop {
  val opStr = "not"
  def copyWithContext(c: Context) = copy(ctx = c)
}

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null, ctx: Context = null) extends Expression {
  def copyWithContext(c: Context) = copy(symbol = null, ctx = c)
  def gatherFields = Seq((this, false))
  def sql = Seq(qualifier, Some(name)).flatten.mkString(".")
}

trait SqlAgg extends Expression
case class CountStar(ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = Seq.empty
  def sql = "count(*)"
}
case class CountExpr(expr: Expression, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sql = Seq(Some("count("), Some(expr.sql), Some(")")).flatten.mkString("")
}
case class Sum(expr: Expression, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sql = Seq(Some("sum("), Some(expr.sql), Some(")")).flatten.mkString("")
}
case class Avg(expr: Expression, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sql = Seq(Some("avg("), Some(expr.sql), Some(")")).flatten.mkString("")
}
case class Min(expr: Expression, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sql = "min(" + expr.sql + ")"
}
case class Max(expr: Expression, ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
  def sql = "max(" + expr.sql + ")"
}
case class AggCall(name: String, args: Seq[Expression], ctx: Context = null) extends SqlAgg {
  def copyWithContext(c: Context) = copy(ctx = c)
  def gatherFields = args.flatMap(_.gatherFields)
  def sql = Seq(name, "(", args.map(_.sql).mkString(", "), ")").mkString("")
}

trait SqlFunction extends Expression {
  val name: String
  val args: Seq[Expression]
  override def isLiteral = args.foldLeft(true)(_ && _.isLiteral)
  def gatherFields = args.flatMap(_.gatherFields)
  def sql = Seq(name, "(", args.map(_.sql) mkString ", ", ")") mkString ""
}

case class FunctionCall(name: String, args: Seq[Expression], ctx: Context = null) extends SqlFunction {
  def copyWithContext(c: Context) = copy(ctx = c)
}

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType


case class UnaryPlus(expr: Expression, ctx: Context = null) extends Unop {
  val opStr = "+"
  def copyWithContext(c: Context) = copy(ctx = c)
}
case class UnaryMinus(expr: Expression, ctx: Context = null) extends Unop {
  val opStr = "-"
  def copyWithContext(c: Context) = copy(ctx = c)
}

trait LiteralExpr extends Expression {
  override def isLiteral = true
  def gatherFields = Seq.empty
}
case class IntLiteral(v: Long, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = v.toString
}
case class FloatLiteral(v: Double, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = v.toString
}
case class StringLiteral(v: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = "\"" + v.toString + "\"" // TODO: escape...
}
case class NullLiteral(ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = "null"
}
case class DateLiteral(d: String, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = Seq("date", "\"" + d + "\"") mkString " "
}
case class IntervalLiteral(e: String, unit: ExtractType, ctx: Context = null) extends LiteralExpr {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = Seq("interval", "\"" + e + "\"", unit.toString) mkString " "
}

case class Table(name: String, alias: Option[String], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = Seq(Some(name), alias).flatten.mkString(" ")

}

case class GroupBy(keys: Seq[Expression], ctx: Context = null) extends Node {
  def copyWithContext(c: Context) = copy(ctx = c)
  def sql = Seq(Some("group by"), Some(keys.map(_.sql).mkString(", "))).flatten.mkString(" ")
}
