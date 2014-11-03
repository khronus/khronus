package com.despegar.metrik.web.service.influx.parser

import java.util.concurrent.TimeUnit

import com.despegar.metrik.util.Logging

import scala.concurrent.duration.FiniteDuration
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import com.despegar.metrik.model.Functions

class InfluxQueryParser extends StandardTokenParsers with Logging {

  class InfluxLexical extends StdLexical

  override val lexical = new InfluxLexical

  val functions = Functions.allValuesAsString

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "group_by_time", "limit", "between", "null", "date", "time", "now", "order", "asc", "desc",
    TimeSuffixes.Seconds, TimeSuffixes.Minutes, TimeSuffixes.Hours, TimeSuffixes.Days, TimeSuffixes.Weeks)

  lexical.reserved ++= functions

  lexical.delimiters += ("*", Operators.Lt, Operators.Eq, Operators.Neq, Operators.Lte, Operators.Gte, Operators.Gt, "(", ")", ",", ".", ";", "-")

  def parse(influxQuery: String): Option[InfluxCriteria] = {
    log.info(s"Parsing influx query [$influxQuery]")

    // TODO - Hack because of conflict: group by time & time as identifier
    val queryToParse = influxQuery.replace("group by time", "group_by_time")

    phrase(influxQueryParser)(new lexical.Scanner(queryToParse)) match {
      case Success(r, q) ⇒ Option(r)
      case x             ⇒ log.error(s"Error parsing query [$influxQuery]: $x"); None
    }
  }

  private def influxQueryParser: Parser[InfluxCriteria] =
    "select" ~> projectionParser ~
      tableParser ~ opt(filterParser) ~
      groupByParser ~ opt(limitParser) ~ opt(orderParser) <~ opt(";") ^^ {
        case projection ~ table ~ filters ~ groupBy ~ limit ~ order ⇒ InfluxCriteria(projection, table, filters.getOrElse(Nil), groupBy, limit, order.getOrElse(true))
      }

  private def projectionParser: Parser[Seq[Projection]] =
    allFieldProjectionParser |
      rep(projectionExpressionParser).map(x ⇒ x.flatten)

  private def allFieldProjectionParser: Parser[Seq[Projection]] = "*" ^^ (_ ⇒ Seq(AllField()))

  private def projectionExpressionParser: Parser[Seq[Projection]] = {
    projectionFieldOrFunctionParser ~ opt("as" ~> ident) ~ opt(",") ^^ {
      case x ~ alias ~ _ ⇒ {
        x match {
          case id: Identifier             ⇒ Seq(Field(id.value, alias))
          case proj: ProjectionExpression ⇒ Seq(Field(proj.function, alias))
        }
      }
    }
  }

  private def projectionFieldOrFunctionParser: Parser[Expression] =
    ident ^^ (Identifier(_)) |
      knownFunctionParser

  private def knownFunctionParser: Parser[Expression] =
    Functions.Count.value ~> "(" ~> ident <~ ")" ^^ (Count(_)) |
      Functions.Min.value ~> "(" ~> ident <~ ")" ^^ (Min(_)) |
      Functions.Max.value ~> "(" ~> ident <~ ")" ^^ (Max(_)) |
      Functions.Avg.value ~> "(" ~> ident <~ ")" ^^ (Avg(_)) |
      Functions.Percentile50.value ~> "(" ~> ident <~ ")" ^^ (Percentile50(_)) |
      Functions.Percentile80.value ~> "(" ~> ident <~ ")" ^^ (Percentile80(_)) |
      Functions.Percentile90.value ~> "(" ~> ident <~ ")" ^^ (Percentile90(_)) |
      Functions.Percentile95.value ~> "(" ~> ident <~ ")" ^^ (Percentile95(_)) |
      Functions.Percentile99.value ~> "(" ~> ident <~ ")" ^^ (Percentile99(_)) |
      Functions.Percentile999.value ~> "(" ~> ident <~ ")" ^^ (Percentile999(_))

  private def tableParser: Parser[Table] =
    "from" ~> ident ~ opt("as") ~ opt(ident) ^^ {
      case ident ~ _ ~ alias ⇒ Table(ident, alias)
    }

  private def filterParser: Parser[List[Filter]] = "where" ~> filterExpression

  private def filterExpression: Parser[List[Filter]] = rep(
    stringComparatorExpression |
      timestampComparatorExpression |
      timeBetweenExpression |
      relativeTimeExpression).map(x ⇒ x.flatten)

  def stringComparatorExpression: Parser[List[StringFilter]] = {
    ident ~ (Operators.Eq | Operators.Neq) ~ stringParser <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ strValue ⇒ List(StringFilter(identifier, operator, strValue))
    }
  }

  def timestampComparatorExpression: Parser[List[TimeFilter]] = {
    "time" ~ (Operators.Lt | Operators.Lte | Operators.Gt | Operators.Gte) ~ timeWithSuffixToMillisParser <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ timeInMillis ⇒ List(TimeFilter(identifier, operator, timeInMillis))
    }
  }

  def timeBetweenExpression: Parser[List[TimeFilter]] = {
    "time" ~ "between" ~ timeWithSuffixToMillisParser ~ "and" ~ timeWithSuffixToMillisParser <~ opt(Operators.And) ^^ {
      case identifier ~ _ ~ millisA ~ _ ~ millisB ⇒ List(TimeFilter(identifier, Operators.Gte, millisA), TimeFilter(identifier, Operators.Lte, millisB))
    }
  }

  def relativeTimeExpression: Parser[List[TimeFilter]] = {
    "time" ~ (Operators.Lt | Operators.Lte | Operators.Gt | Operators.Gte) ~ "now" ~ "(" ~ ")" ~ opt("-") ~ opt(timeWithSuffixToMillisParser) <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ _ ~ _ ~ _ ~ _ ~ timeInMillis ⇒ {
        List(TimeFilter(identifier, operator, now - timeInMillis.getOrElse(0L)))
      }
    }
  }

  private def timeWithSuffixToMillisParser: Parser[Long] = {
    numericLit ~ opt(TimeSuffixes.Seconds | TimeSuffixes.Minutes | TimeSuffixes.Hours | TimeSuffixes.Days | TimeSuffixes.Weeks) ^^ {
      case number ~ timeUnit ⇒
        timeUnit.map { toMillis(number, _) }.getOrElse(number.toLong)
    }
  }

  private def toMillis(number: String, suffix: String): Long = suffix match {
    case TimeSuffixes.Seconds ⇒ TimeUnit.SECONDS.toMillis(number.toLong)
    case TimeSuffixes.Minutes ⇒ TimeUnit.MINUTES.toMillis(number.toLong)
    case TimeSuffixes.Hours   ⇒ TimeUnit.HOURS.toMillis(number.toLong)
    case TimeSuffixes.Days    ⇒ TimeUnit.DAYS.toMillis(number.toLong)
    case TimeSuffixes.Weeks   ⇒ TimeUnit.DAYS.toMillis(number.toLong) * 7L
    case _                    ⇒ number.toLong
  }

  private def groupByParser: Parser[GroupBy] =
    "group_by_time" ~> "(" ~> timeWindowParser <~ ")" ^^ (GroupBy(_))

  private def timeWindowParser: Parser[FiniteDuration] =
    ((numberEqualTo(30) ~ TimeSuffixes.Seconds) | (numberEqualTo(1) ~ TimeSuffixes.Minutes) | (numberEqualTo(5) ~ TimeSuffixes.Minutes) |
      (numberEqualTo(10) ~ TimeSuffixes.Minutes) | (numberEqualTo(30) ~ TimeSuffixes.Minutes) | (numberEqualTo(1) ~ TimeSuffixes.Hours)) ^^ {
        case number ~ timeUnit ⇒ {
          timeUnit match {
            case TimeSuffixes.Seconds ⇒ new FiniteDuration(number.toLong, TimeUnit.SECONDS)
            case TimeSuffixes.Minutes ⇒ new FiniteDuration(number.toLong, TimeUnit.MINUTES)
            case TimeSuffixes.Hours   ⇒ new FiniteDuration(number.toLong, TimeUnit.HOURS)
          }
        }
      }

  private def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def orderParser: Parser[Boolean] = "order" ~> ("asc" | "desc") ^^ { case o ⇒ "asc".equals(o) }

  protected def now: Long = System.currentTimeMillis()

  private def numberEqualTo(n: Int): Parser[Int] =
    elem(s"Expected number $n", _.toString == n.toString) ^^ (_.toString.toInt)

  private def stringParser: Parser[String] = stringLit ^^ {
    case s ⇒ s
  }

}
