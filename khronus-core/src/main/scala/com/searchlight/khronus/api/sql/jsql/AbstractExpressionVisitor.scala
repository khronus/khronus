package com.searchlight.khronus.api.sql.jsql

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional.{ AndExpression, OrExpression }
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.SubSelect

class AbstractExpressionVisitor extends ExpressionVisitor {
  override def visit(allComparisonExpression: AllComparisonExpression): Unit = ???

  override def visit(existsExpression: ExistsExpression): Unit = ???

  override def visit(whenClause: WhenClause): Unit = ???

  override def visit(caseExpression: CaseExpression): Unit = ???

  override def visit(subSelect: SubSelect): Unit = ???

  override def visit(tableColumn: Column): Unit = ???

  override def visit(notEqualsTo: NotEqualsTo): Unit = ???

  override def visit(aexpr: AnalyticExpression): Unit = ???

  override def visit(wgexpr: WithinGroupExpression): Unit = ???

  override def visit(eexpr: ExtractExpression): Unit = ???

  override def visit(iexpr: IntervalExpression): Unit = ???

  override def visit(oexpr: OracleHierarchicalExpression): Unit = ???

  override def visit(rexpr: RegExpMatchOperator): Unit = ???

  override def visit(addition: Addition): Unit = ???

  override def visit(division: Division): Unit = ???

  override def visit(multiplication: Multiplication): Unit = ???

  override def visit(subtraction: Subtraction): Unit = ???

  override def visit(andExpression: AndExpression): Unit = ???

  override def visit(orExpression: OrExpression): Unit = ???

  override def visit(cast: CastExpression): Unit = ???

  override def visit(bitwiseXor: BitwiseXor): Unit = ???

  override def visit(bitwiseOr: BitwiseOr): Unit = ???

  override def visit(bitwiseAnd: BitwiseAnd): Unit = ???

  override def visit(matches: Matches): Unit = ???

  override def visit(concat: Concat): Unit = ???

  override def visit(anyComparisonExpression: AnyComparisonExpression): Unit = ???

  override def visit(regExpMySQLOperator: RegExpMySQLOperator): Unit = ???

  override def visit(`var`: UserVariable): Unit = ???

  override def visit(bind: NumericBind): Unit = ???

  override def visit(aexpr: KeepExpression): Unit = ???

  override def visit(groupConcat: MySQLGroupConcat): Unit = ???

  override def visit(rowConstructor: RowConstructor): Unit = ???

  override def visit(modulo: Modulo): Unit = ???

  override def visit(doubleValue: DoubleValue): Unit = ???

  override def visit(jdbcNamedParameter: JdbcNamedParameter): Unit = ???

  override def visit(jdbcParameter: JdbcParameter): Unit = ???

  override def visit(signedExpression: SignedExpression): Unit = ???

  override def visit(function: Function): Unit = ???

  override def visit(nullValue: NullValue): Unit = ???

  override def visit(jsonExpr: JsonExpression): Unit = ???

  override def visit(isNullExpression: IsNullExpression): Unit = ???

  override def visit(inExpression: InExpression): Unit = ???

  override def visit(greaterThanEquals: GreaterThanEquals): Unit = ???

  override def visit(greaterThan: GreaterThan): Unit = ???

  override def visit(equalsTo: EqualsTo): Unit = ???

  override def visit(between: Between): Unit = ???

  override def visit(longValue: LongValue): Unit = ???

  override def visit(hexValue: HexValue): Unit = ???

  override def visit(dateValue: DateValue): Unit = ???

  override def visit(timeValue: TimeValue): Unit = ???

  override def visit(timestampValue: TimestampValue): Unit = ???

  override def visit(parenthesis: Parenthesis): Unit = ???

  override def visit(stringValue: StringValue): Unit = ???

  override def visit(likeExpression: LikeExpression): Unit = ???

  override def visit(minorThan: MinorThan): Unit = ???

  override def visit(minorThanEquals: MinorThanEquals): Unit = ???

  override def visit(hint: OracleHint): Unit = ???
}
