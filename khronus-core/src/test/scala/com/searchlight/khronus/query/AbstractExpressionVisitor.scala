package com.searchlight.khronus.query

import java.util

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.operators.relational.{GreaterThan => GreaterThanExpression, MinorThan => MinorThanExpression, _}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.SubSelect

import scala.collection.mutable

import scala.collection.JavaConverters._

case class BinaryOperation(alias: String, tag: String, value: String)

class ListVisitor extends ItemsListVisitor {
  val values = mutable.Buffer[String]()

  override def visit(subSelect: SubSelect): Unit = ???

  override def visit(expressionList: ExpressionList): Unit = {
    if (expressionList.getExpressions.isInstanceOf[util.List[StringValue]]){
      values ++= expressionList.getExpressions.asInstanceOf[util.List[StringValue]].asScala map (_.getValue)
    }
  }

  override def visit(multiExprList: MultiExpressionList): Unit = ???
}

class PredicateVisitor extends AbstractExpressionVisitor {
  val predicates = mutable.Buffer[Predicate]()

  override def visit(equalsTo: EqualsTo): Unit = {
    val bin = binaryOperator(equalsTo)
    predicates +=  Equals(bin.alias, bin.tag, bin.value)
  }

  override def visit(andExpression: AndExpression): Unit = {
    andExpression.getLeftExpression.accept(this)
    andExpression.getRightExpression.accept(this)
  }

  override def visit(minorThan: MinorThanExpression): Unit = {
    val bin = binaryOperator(minorThan)
    predicates +=  MinorThan(bin.alias, bin.tag, bin.value.toLong)
  }

  override def visit(greaterThan: GreaterThanExpression): Unit = {
     val bin = binaryOperator(greaterThan)
     predicates +=  GreaterThan(bin.alias, bin.tag, bin.value.toLong)
  }

  override def visit(inExpression: InExpression): Unit = {
    val listVisitor = new ListVisitor()
    inExpression.getRightItemsList.accept(listVisitor)

    inExpression.getLeftExpression match {
      case e: Column => predicates += In(e.getTable.getName, e.getColumnName.toString, listVisitor.values.toList)
    }
  }

  private def binaryOperator(expression: BinaryExpression): BinaryOperation = {
    val value = expression.getRightExpression match {
      case e: LongValue => e.getValue.toString
    }

    expression.getLeftExpression match {
      case e: Column => BinaryOperation(e.getTable.getName, e.getColumnName.toString, value)
    }
  }
}

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

  override def visit(greaterThan: GreaterThanExpression): Unit = ???

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

  override def visit(minorThan: MinorThanExpression): Unit = ???

  override def visit(minorThanEquals: MinorThanEquals): Unit = ???
}
