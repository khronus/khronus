package com.searchlight.khronus.api.sql.jsql

import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.expression.operators.relational.{ ExpressionList, ItemsListVisitor, MultiExpressionList }
import net.sf.jsqlparser.statement.select.SubSelect

import scala.collection.JavaConverters._
import scala.collection.mutable

class ListVisitor extends ItemsListVisitor {
  val values = mutable.Buffer[String]()

  override def visit(subSelect: SubSelect): Unit = ???

  override def visit(expressionList: ExpressionList): Unit = {
    values ++= expressionList.getExpressions.asScala.collect { case stringValue: StringValue ⇒ stringValue.getValue }
  }

  override def visit(multiExprList: MultiExpressionList): Unit = ???
}
