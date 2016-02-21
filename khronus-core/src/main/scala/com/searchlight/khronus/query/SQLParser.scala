package com.searchlight.khronus.query

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.operators.arithmetic.Division
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{ Column, Table }
import net.sf.jsqlparser.statement.select.{ FromItem, PlainSelect, Select, SelectExpressionItem }
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.matching.Regex.Match

trait SQLParserSupport {
  def parser: SQLParser = SQLParser.instance
}

trait SQLParser {
  def parse(query: String): DynamicQuery
}

object SQLParser {
  val instance = new JSQLParser
}

class JSQLParser extends SQLParser {

  private val functionFactories: Map[String, ((String, Seq[Expression]) ⇒ Projection)] = Map(
    "count" -> Count.factory, "percentiles" -> Percentiles.factory)

  implicit def fromFromItemToQMetric(fromItem: FromItem): QMetric = {
    fromItem match {
      case item: Table ⇒ QMetric(item.getName, item.getAlias.getName)
    }
  }

  private def metrics(plainSelect: PlainSelect): Seq[QMetric] = {
    val joins = Option(plainSelect.getJoins)
    Seq[QMetric](plainSelect.getFromItem) ++ {
      joins.map(j ⇒ j.asScala.map { join ⇒ val item: QMetric = join.getRightItem; item }).getOrElse(Seq())
    }
  }

  def projections(plainSelect: PlainSelect): Seq[Projection] = {
    plainSelect.getSelectItems.asScala.map {
      case selectItem: SelectExpressionItem ⇒ projection(selectItem.getExpression)
    }.toSeq
  }

  private def projection(expression: Expression): Projection = {
    expression match {
      case d: Division ⇒ {
        val left = d.getLeftExpression
        val right = d.getRightExpression
        DivProjection(projection(left), projection(right))
      }
      case f: net.sf.jsqlparser.expression.Function ⇒ {
        val params = f.getParameters.getExpressions.asScala
        val head = params.head
        val tail = params.tail
        functionFactories(f.getName)(head match { case exp: Column ⇒ exp.getColumnName }, tail)
      }
    }
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

  val patternTimeRange = """(?:and )?time \>[^0-9]+([0-9]+)[^\<]+\<[^0-9]+([0-9]+)( and)?""".r
  val patternOnlyTimeRange = """where time >[^\<]+\<[^0-9]+[0-9]+$""".r
  val resolutionPattern = """(time)\s*\(([0-9]h)\)""".r

  private def replaceResolution(m: Match) = s"groupby${m.group(1)}.T${m.group(2)}"

  private def resolution(plainSelect: PlainSelect): Option[Duration] = {
    Option(plainSelect.getGroupByColumnReferences).flatMap(_.asScala.find { case c: Column ⇒ c.getTable.getName.equals("groupbytime") }.map {
      e ⇒
        e.asInstanceOf[Column].getColumnName match {
          case "T1h"  ⇒ 1 hour
          case "T1m"  ⇒ 1 minute
          case "T30s" ⇒ 30 seconds
          case "T5m"  ⇒ 5 minute
          case "T10m" ⇒ 10 minute
          case "T30m" ⇒ 30 minute
          case _      ⇒ 1 minute
        }
    })
  }

  def parse(sql: String): DynamicQuery = {
    val sqlWithoutTimeRange = patternOnlyTimeRange.findFirstIn(sql.trim) match {
      case Some(_) ⇒ patternOnlyTimeRange.replaceAllIn(sql.trim, "")
      case None    ⇒ patternTimeRange.replaceAllIn(sql, "")
    }
    val sqlWithResolutionFixed = resolutionPattern.replaceAllIn(sqlWithoutTimeRange, replaceResolution _)

    val stmt = CCJSqlParserUtil.parse(sqlWithResolutionFixed).asInstanceOf[Select]
    val plainSelect: PlainSelect = stmt.getSelectBody.asInstanceOf[PlainSelect]
    DynamicQuery(projections(plainSelect), metrics(plainSelect), predicate(plainSelect), timeRange(sql), resolution(plainSelect))
  }
}

