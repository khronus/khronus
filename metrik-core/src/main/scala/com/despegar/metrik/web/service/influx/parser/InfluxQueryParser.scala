package com.despegar.metrik.web.service.influx.parser

import java.util.concurrent.TimeUnit

import com.despegar.metrik.util.{ Settings, Logging }

import scala.concurrent.duration.FiniteDuration
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import com.despegar.metrik.model.Functions

class InfluxQueryParser extends StandardTokenParsers with Logging {

  class InfluxLexical extends StdLexical

  override val lexical = new InfluxLexical

  val functions = Functions.allNames

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "group_by_time", "limit", "between", "null", "date", "time", "now", "order", "asc", "desc", "percentiles",
    TimeSuffixes.Seconds, TimeSuffixes.Minutes, TimeSuffixes.Hours, TimeSuffixes.Days, TimeSuffixes.Weeks)

  lexical.reserved ++= functions

  lexical.delimiters += ("*", Operators.Lt, Operators.Eq, Operators.Neq, Operators.Lte, Operators.Gte, Operators.Gt, "(", ")", ",", ".", ";", "-")

  def parse(influxQuery: String): InfluxCriteria = {
    log.info(s"Parsing influx query [$influxQuery]")

    // TODO - Hack because of conflict: group by time & time as identifier
    val queryToParse = influxQuery.replace("group by time", "group_by_time")

    phrase(influxQueryParser)(new lexical.Scanner(queryToParse)) match {
      case Success(r, q) ⇒ r
      case x             ⇒ log.error(s"Error parsing query [$influxQuery]: $x"); throw new UnsupportedOperationException(s"Unsupported query [$influxQuery]: $x")
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
    (ident ^^ (Identifier(_)) | knownFunctionParser | percentilesFunctionParser) ~ opt("as" ~> ident) ~ opt(",") ^^ {
      case x ~ alias ~ _ ⇒ {
        x match {
          case id: Identifier                                ⇒ Seq(Field(id.value, alias))
          case functions: Seq[Functions.Function @unchecked] ⇒ functions.map(f ⇒ Field(f.name, alias))
        }
      }
    }
  }

  private def knownFunctionParser: Parser[Seq[Functions.Function]] = {
    elem(s"Expected some function", { e ⇒ Functions.allNames.contains(e.chars.toString) }) <~ "(" <~ ident <~ ")" ^^ {
      case f ⇒ Seq(Functions.withName(f.chars.toString))
    }
  }

  private def percentilesFunctionParser: Parser[Seq[Functions.Function]] = {
    "percentiles" ~> "(" ~> rep(validPercentilesParser) <~ ")" ^^ {
      case Nil                 ⇒ Functions.allPercentiles
      case selectedPercentiles ⇒ selectedPercentiles.collect { case p ⇒ Functions.percentileByValue(p) }
    }
  }

  private def validPercentilesParser: Parser[Int] =
    elem(s"Expected some valid percentile", { e ⇒ e.chars.forall(_.isDigit) && Functions.allPercentilesValues.contains(e.chars.toInt) }) ^^ (_.chars.toInt)

  private def tableParser: Parser[Table] =
    "from" ~> stringLit ~ opt("as") ~ opt(ident) ^^ {
      case metricName ~ _ ~ alias ⇒ Table(metricName, alias)
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
    (numericLit ~ (TimeSuffixes.Seconds | TimeSuffixes.Minutes | TimeSuffixes.Hours)) ^^ {
      case number ~ timeSuffix ⇒ {
        val window = timeSuffix match {
          case TimeSuffixes.Seconds ⇒ new FiniteDuration(number.toLong, TimeUnit.SECONDS)
          case TimeSuffixes.Minutes ⇒ new FiniteDuration(number.toLong, TimeUnit.MINUTES)
          case TimeSuffixes.Hours   ⇒ new FiniteDuration(number.toLong, TimeUnit.HOURS)
        }

        if (!getConfiguredWindows.contains(window)) {
          throw new UnsupportedOperationException(s"Unknown time window [$number$timeSuffix]")
        }

        window
      }
    }

  private def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def orderParser: Parser[Boolean] = "order" ~> ("asc" | "desc") ^^ { case o ⇒ "asc".equals(o) }

  protected def now: Long = System.currentTimeMillis()

  protected def getConfiguredWindows: Seq[FiniteDuration] = {
    Settings().Histogram.configuredWindows.toSeq
  }

  private def stringParser: Parser[String] = stringLit ^^ {
    case s ⇒ s
  }

}
