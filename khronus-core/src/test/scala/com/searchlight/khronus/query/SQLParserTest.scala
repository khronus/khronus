package com.searchlight.khronus.query

import com.searchlight.khronus.util.Test
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{ Column, Table }
import net.sf.jsqlparser.statement.select.{ FromItem, PlainSelect, Select, SelectExpressionItem }

import scala.collection.JavaConverters._

class SQLParserTest extends Test {

  test("parse sql with IN clause") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 23 and m1.country in ('AR','BR') and time > 1 and time < 10 "
    val query = SQLParser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), Some(And(Equals("m1", "tag1", "23"), In("m1", "country", List("AR", "BR")))), TimeRange(1, 10)))
  }

  test("parse most simple sql") {
    val sql = "select count(m1) from metric1 m1 where time > 1 and time < 10 "
    val query = SQLParser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), None, TimeRange(1, 10)))
  }

  test("parse sql with multiples AND predicates") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 2016 and m1.tag2 = 'AR' and m1.tag3 = 'BUE' and m1.tag4 = 'Khronus' time > 1 and time < 10 "
    val query = SQLParser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), Some(And(And(And(Equals("m1", "tag1", "2016"), Equals("m1", "tag2", "AR")), Equals("m1", "tag3", "BUE")), Equals("m1", "tag4", "Khronus"))), TimeRange(1, 10)))
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

  def predicate(plainSelect: PlainSelect): Option[Predicate] = {
    if (plainSelect.getWhere != null) {
      val predicateVisitor = new PredicateVisitor()
      plainSelect.getWhere.accept(predicateVisitor)
      return Some(predicateVisitor.predicates.head)
    }

    None
  }

  def timeRange(sql: String): TimeRange = {
    //and time > 1 and time < 10
    patternTimeRange.findFirstMatchIn(sql) match {
      case Some(m) ⇒ TimeRange(m.group(1).toLong, m.group(2).toLong)
      case None    ⇒ throw new IllegalArgumentException("Invalid time range")
    }
  }

  val patternTimeRange = """(?:and )?time \>[^0-9]+([0-9]+)[^\<]+\<[^0-9]+([0-9]+)(.+$)""".r
  val patternOnlyTimeRange = """where time >[^\<]+\<[^0-9]+[0-9]+$""".r

  def parse(sql: String): DynamicQuery = {
    val sqlWithoutTimeRange = patternOnlyTimeRange.findFirstIn(sql.trim) match {
      case Some(_) ⇒ patternOnlyTimeRange.replaceAllIn(sql.trim, "")
      case None    ⇒ patternTimeRange.replaceAllIn(sql, "")
    }

    val stmt = CCJSqlParserUtil.parse(sqlWithoutTimeRange).asInstanceOf[Select]
    val plainSelect: PlainSelect = stmt.getSelectBody.asInstanceOf[PlainSelect]
    DynamicQuery(projections(plainSelect), metrics(plainSelect), predicate(plainSelect), timeRange(sql))
  }
}