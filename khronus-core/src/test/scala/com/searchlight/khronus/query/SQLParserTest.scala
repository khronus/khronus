package com.searchlight.khronus.query

import com.searchlight.khronus.util.Test
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{ Column, Table }
import net.sf.jsqlparser.statement.select.{ FromItem, PlainSelect, Select, SelectExpressionItem }

import scala.collection.JavaConverters._

class SQLParserTest extends Test {

  test("parse sql") {
    //val sql = "select count(m1) from metric1 m1 where m1.tag1 = value1 and time > 1 and time < 10"
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 23 and m2.country in ('AR','BR') and time > 1 and time < 10 "
    val query = SQLParser.parse(sql)
    //query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1","m1")), Equals("m1","tag1","value1"), TimeRange(1,2)))
  }
}

object SQLParser {

  private val functionFactories: Map[String, (Seq[String] ⇒ Projection)] = Map(
    "count" -> ((params: Seq[String]) ⇒ Count(params.head)))

  private def metrics(plainSelect: PlainSelect): Seq[QMetric] = {
    val item: FromItem = plainSelect.getFromItem
    Seq(QMetric(item.asInstanceOf[Table].getName, item.getAlias.getName))
  }

  def projections(plainSelect: PlainSelect): Seq[Projection] = {
    plainSelect.getSelectItems.asScala.map {
      case selectItem: SelectExpressionItem ⇒
        selectItem.getExpression match {
          case f: net.sf.jsqlparser.expression.Function ⇒ {
            val params = f.getParameters.getExpressions.asScala.map { case exp: Column ⇒ exp.getColumnName }
            functionFactories(f.getName)(params)
          }
        }
    }.toSeq
  }

  def predicate(plainSelect: PlainSelect): Predicate = {
    val predicateVisitor = new PredicateVisitor()
    plainSelect.getWhere.accept(predicateVisitor)
    println(s"<--- PREDICATES:  ${predicateVisitor.predicates}")
    null
  }

  def timeRange(plainSelect: PlainSelect): TimeRange = null

  def parse(sql: String): DynamicQuery = {
    val stmt = CCJSqlParserUtil.parse(sql).asInstanceOf[Select]
    val plainSelect: PlainSelect = stmt.getSelectBody.asInstanceOf[PlainSelect]
    DynamicQuery(projections(plainSelect), metrics(plainSelect), predicate(plainSelect), timeRange(plainSelect))
  }
}