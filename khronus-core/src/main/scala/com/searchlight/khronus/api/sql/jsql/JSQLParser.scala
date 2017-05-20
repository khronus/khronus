package com.searchlight.khronus.api.sql.jsql

import com.searchlight.khronus.api.sql.SQLParser
import com.searchlight.khronus.model.query._
import net.sf.jsqlparser.expression.operators.arithmetic.Division
import net.sf.jsqlparser.expression.{ Expression, DoubleValue, LongValue }
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{ Column, Table }
import net.sf.jsqlparser.statement.select.{ FromItem, PlainSelect, Select, SelectExpressionItem }

import scala.concurrent.duration._
import scala.util.matching.Regex.Match
import collection.JavaConverters._

class JSQLParser extends SQLParser {

  implicit def fromFromItemToSelector(fromItem: FromItem): Selector = {
    fromItem match {
      case item: Table ⇒ Selector(item.getName, Option(item.getAlias).map(_.getName))
    }
  }

  private def parseSelectors(plainSelect: PlainSelect): Seq[Selector] = {
    val joins = Option(plainSelect.getJoins)
    Seq[Selector](plainSelect.getFromItem) ++ {
      joins.map(j ⇒ j.asScala.map { join ⇒ val item: Selector = join.getRightItem; item }).getOrElse(Seq())
    }
  }

  def projections(plainSelect: PlainSelect, selectors: Seq[Selector]): Seq[Projection] = {
    plainSelect.getSelectItems.asScala.flatMap {
      case selectItem: SelectExpressionItem ⇒ projection(selectItem.getExpression, selectors)
    }.toSeq
  }

  def findSelector(selectors: Seq[Selector], params: Seq[Expression]): Selector = {
    params.headOption match {
      case Some(firstParam: Column) if !firstParam.getColumnName.equals("value") ⇒ SQLParser.selector(firstParam.getColumnName, selectors)
      case _ if selectors.tail.isEmpty ⇒ selectors.head
      case _ ⇒ throw new IllegalArgumentException("Should specify selector")
    }
  }

  private def projection(expression: Expression, selectors: Seq[Selector]): Seq[Projection] = {
    expression match {
      case d: Division ⇒ {
        val left = d.getLeftExpression
        val right = d.getRightExpression
        val leftProjections: Seq[Projection] = projection(left, selectors)
        val rightProjections: Seq[Projection] = projection(right, selectors)
        leftProjections.zip(rightProjections).map { case (leftProjection, rightProjection) ⇒ Div(leftProjection, rightProjection) }
      }
      case function: net.sf.jsqlparser.expression.Function ⇒ {
        val parameters = Option(function.getParameters).map(_.getExpressions.asScala).getOrElse(Seq())
        val selector: Selector = findSelector(selectors, parameters)
        function.getName match {
          case "count" ⇒ Seq(Count(selector))
          case "percentiles" ⇒ parameters.tail.map {
            case percentile: LongValue   ⇒ Percentile(selector, percentile.getValue.toDouble)
            case percentile: DoubleValue ⇒ Percentile(selector, percentile.getValue)
            case _                       ⇒ ???
          }
          case _ ⇒ throw new UnsupportedOperationException(function.getName)
        }

      }
    }
  }

  def predicate(plainSelect: PlainSelect, selectors: Seq[Selector]): Option[Filter] = {
    val predicateVisitor = new PredicateVisitor(selectors)
    if (plainSelect.getWhere != null) {
      plainSelect.getWhere.accept(predicateVisitor)
    }
    predicateVisitor.predicates.headOption
  }

  def parseTimeExpression(someMatch: Match): TimeRange = {
    val sign = someMatch.group(1)
    val time = Option(someMatch.group(3)).map(n ⇒ now(someMatch.group(4), someMatch.group(5))).getOrElse(someMatch.group(2)).toLong
    sign match {
      case ">"  ⇒ TimeRange(time, System.currentTimeMillis())
      case ">=" ⇒ TimeRange(time, System.currentTimeMillis())
      case "<"  ⇒ TimeRange(1, time)
      case "<=" ⇒ TimeRange(1, time)
    }
  }

  def timeRange(sql: String): Option[TimeRange] = {
    val timeFilter = timePattern.findAllMatchIn(sql).toSeq
    timeFilter.size match {
      case 1 ⇒ Some(parseTimeExpression(timeFilter.head))
      case 2 ⇒ Some(parseTimeExpression(timeFilter.head) mergedWith parseTimeExpression(timeFilter.last))
      case _ ⇒ None
    }
  }

  val timePattern = """time\s+(<|<=|>|>=)\s+([0-9]+|(now\(\))\s+(-|\+)\s+([0-9]+[shmdwMY]))""".r
  val resolutionPattern = """(time)\s*\(([0-9]+[shmdwMY])\)""".r
  val durationPattern = "([0-9]+)([shmdwMY])".r

  private def replaceResolution(m: Match) = s"groupby${m.group(1)}.T${m.group(2)}"

  private def resolution(plainSelect: PlainSelect): Option[Duration] = {
    //TODO: refactor me
    Option(plainSelect.getGroupByColumnReferences).flatMap(_.asScala.find { case c: Column ⇒ c.getTable.getName.equals("groupbytime") }.map {
      e ⇒
        fromStringToDuration(e.asInstanceOf[Column].getColumnName.replaceAll("T", ""))
    })
  }

  private def fromStringToDuration(string: String) = {
    val durationPattern(amount, unit) = string
    val n = amount.toInt
    unit match {
      case "s" ⇒ n seconds
      case "m" ⇒ n minutes
      case "h" ⇒ n hours
      case "d" ⇒ n days
      case "w" ⇒ (n * 7) days
      case "M" ⇒ (n * 30) days
      case "Y" ⇒ (n * 365) days
    }
  }

  def parse(sql: String): Query = {
    val preProcessedSQL: String = preprocess(sql)
    val stmt = CCJSqlParserUtil.parse(preProcessedSQL).asInstanceOf[Select]
    val plainSelect: PlainSelect = stmt.getSelectBody.asInstanceOf[PlainSelect]
    val selectors: Seq[Selector] = parseSelectors(plainSelect)
    Query(projections(plainSelect, selectors), selectors, timeRange(sql), predicate(plainSelect, selectors), resolution(plainSelect))
  }

  private def now(sign: String, stringDuration: String): String = {
    val current = System.currentTimeMillis()
    val duration = fromStringToDuration(stringDuration).toMillis
    val time = sign match {
      case "+" ⇒ current + duration
      case "-" ⇒ current - duration
    }
    time toString
  }

  def preprocess(sql: String): String = {
    //TODO: refactor me
    val sqlWithoutTimeRange = timePattern.replaceAllIn(sql, "time=0")

    val sqlWithResolutionFixed = resolutionPattern.replaceAllIn(sqlWithoutTimeRange, replaceResolution _)
    sqlWithResolutionFixed.replaceAll("order (asc|dsc)", "").replaceAll("from \"", "from ").replaceAll("\" group", " group")
      .replaceAll("\" where", " where")
  }
}
