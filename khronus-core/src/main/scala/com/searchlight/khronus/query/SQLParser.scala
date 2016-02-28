package com.searchlight.khronus.query

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.operators.arithmetic.Division
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{ Column, Table }
import net.sf.jsqlparser.statement.select.{ FromItem, PlainSelect, Select, SelectExpressionItem }
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.matching.Regex
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
    val predicateVisitor = new PredicateVisitor()
    if (plainSelect.getWhere != null) {
      plainSelect.getWhere.accept(predicateVisitor)
    }
    predicateVisitor.predicates.headOption
  }

  def parseTimeExpression(someMatch: Match): TimeRange = {
    val sign = someMatch.group(1)
    val time = Option(someMatch.group(3)).map(n ⇒ now(someMatch.group(4), someMatch.group(5))).getOrElse(someMatch.group(2)).toLong
    sign match {
      case ">"  ⇒ TimeRange(time + 0, System.currentTimeMillis())
      case ">=" ⇒ TimeRange(time, System.currentTimeMillis())
      case "<"  ⇒ TimeRange(1, time - 0)
      case "<=" ⇒ TimeRange(1, time)
    }
  }

  def timeRange(sql: String): TimeRange = {
    val timeFilter = timePattern.findAllMatchIn(sql).toSeq
    timeFilter.size match {
      case 1 ⇒ parseTimeExpression(timeFilter.head)
      case 2 ⇒ parseTimeExpression(timeFilter.head) mergedWith parseTimeExpression(timeFilter.last)
      case _ ⇒ throw new IllegalArgumentException(s"Invalid time range: $timeFilter")
    }
  }

  val timePattern = """time\s+(<|<=|>|>=)\s+([0-9]+|(now\(\))(-|\+)([0-9]+[shmd]))""".r
  val resolutionPattern = """(time)\s*\(([0-9]+[shmd])\)""".r
  val durationPattern = "([0-9]+)([shmd])".r

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
    }
  }

  def parse(sql: String): DynamicQuery = {
    val preProcessedSQL: String = preprocess(sql)
    val stmt = CCJSqlParserUtil.parse(preProcessedSQL).asInstanceOf[Select]
    val plainSelect: PlainSelect = stmt.getSelectBody.asInstanceOf[PlainSelect]
    DynamicQuery(projections(plainSelect), metrics(plainSelect), predicate(plainSelect), timeRange(sql), resolution(plainSelect))
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

