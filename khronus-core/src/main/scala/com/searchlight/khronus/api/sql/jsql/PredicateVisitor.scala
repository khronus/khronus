package com.searchlight.khronus.api.sql.jsql

import com.searchlight.khronus.api.sql.SQLParser
import com.searchlight.khronus.model.query._
import net.sf.jsqlparser.expression.operators.relational.{ GreaterThan ⇒ GreaterThanExpression, MinorThan ⇒ MinorThanExpression, InExpression, EqualsTo }
import net.sf.jsqlparser.expression.{ StringValue, LongValue, BinaryExpression }
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.schema.Column

import scala.collection.mutable

class PredicateVisitor(selectors: Seq[Selector]) extends AbstractExpressionVisitor {
  val predicates = mutable.Buffer[Filter]()

  private def selector(id: String) = SQLParser.selector(id, selectors)

  override def visit(equalsTo: EqualsTo): Unit = {
    val bin = binaryOperator(equalsTo)
    if (!(bin.tag.equals("time") && bin.value.equals("0")))
      predicates += Equals(SQLParser.selector(bin.alias, selectors), bin.tag, bin.value)
  }

  override def visit(andExpression: AndExpression): Unit = {
    val left = new PredicateVisitor(selectors)
    val right = new PredicateVisitor(selectors)
    andExpression.getLeftExpression.accept(left)
    andExpression.getRightExpression.accept(right)
    if (left.predicates.nonEmpty && right.predicates.nonEmpty) {
      predicates += And(Seq(left.predicates.head, right.predicates.head))
    } else {
      if (left.predicates.nonEmpty) {
        predicates += left.predicates.head
      }
      if (right.predicates.nonEmpty) {
        predicates += right.predicates.head
      }
    }
  }

  override def visit(minorThan: MinorThanExpression): Unit = {
    val bin = binaryOperator(minorThan)
    predicates += MinorThan(bin.alias, bin.tag, bin.value.toLong)
  }

  override def visit(greaterThan: GreaterThanExpression): Unit = {
    val bin = binaryOperator(greaterThan)
    predicates += GreaterThan(bin.alias, bin.tag, bin.value.toLong)
  }

  override def visit(inExpression: InExpression): Unit = {
    val listVisitor = new ListVisitor()
    inExpression.getRightItemsList.accept(listVisitor)

    inExpression.getLeftExpression match {
      case e: Column ⇒ predicates += In(selector(e.getTable.getName), e.getColumnName, listVisitor.values.toList)
    }
  }

  private def binaryOperator(expression: BinaryExpression): BinaryOperation = {
    val value = expression.getRightExpression match {
      case l: LongValue   ⇒ l.getValue.toString
      case s: StringValue ⇒ s.getValue
      case c: Column      ⇒ c.getColumnName.replaceAll("\"", "")
    }

    expression.getLeftExpression match {
      case e: Column ⇒ BinaryOperation(e.getTable.getName, e.getColumnName, value)
    }
  }
}
