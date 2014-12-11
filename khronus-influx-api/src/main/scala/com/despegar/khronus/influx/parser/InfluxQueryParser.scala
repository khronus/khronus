/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.despegar.khronus.influx.parser

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import com.despegar.khronus.model.{ Metric, MetricType, Functions }
import com.despegar.khronus.store.{ MetaSupport, MetaStore }
import com.despegar.khronus.util.log.Logging
import scala.concurrent.{ Future, ExecutionContext }
import com.despegar.khronus.util.ConcurrencySupport

class InfluxQueryParser extends StandardTokenParsers with Logging with MetaSupport with ConcurrencySupport {

  class InfluxLexical extends StdLexical

  override val lexical = new InfluxLexical

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "group_by_time", "limit", "between", "null", "date", "time", "now", "order", "asc", "desc", "percentiles", "force",
    TimeSuffixes.Seconds, TimeSuffixes.Minutes, TimeSuffixes.Hours, TimeSuffixes.Days, TimeSuffixes.Weeks)

  lexical.reserved ++= Functions.allNames

  lexical.delimiters += ("*", Operators.Lt, Operators.Eq, Operators.Neq, Operators.Lte, Operators.Gte, Operators.Gt, "(", ")", ",", ".", ";", "-")

  implicit val ex: ExecutionContext = executionContext("influx-query-parser-worker")

  def parse(influxQuery: String): Future[InfluxCriteria] = {
    log.info(s"Parsing influx query [$influxQuery]")

    // TODO - Hack because of conflict: group by time & time as identifier
    val queryToParse = influxQuery.replace("group by time", "group_by_time")

    phrase(influxQueryParser)(new lexical.Scanner(queryToParse)) match {
      case Success(r, q) ⇒ r
      case x             ⇒ log.error(s"Error parsing query [$influxQuery]: $x"); throw new UnsupportedOperationException(s"Unsupported query [$influxQuery]: $x")
    }
  }

  private def influxQueryParser: Parser[Future[InfluxCriteria]] =
    "select" ~> projectionParser ~ tableParser ~ opt(filterParser) ~
      groupByParser ~ opt(limitParser) ~ opt(orderParser) <~ opt(";") ^^ {
        case projections ~ metricNameRegex ~ filters ~ groupBy ~ limit ~ order ⇒
          buildInfluxCriteria(metricNameRegex, projections, filters.getOrElse(Nil), groupBy, order.getOrElse(true), limit.getOrElse(Int.MaxValue))
      }

  private def buildInfluxCriteria(metricNameRegex: String, projections: Seq[Projection], filters: List[Filter], groupBy: GroupBy, order: Boolean, limit: Int): Future[InfluxCriteria] = {
    metaStore.searchInSnapshot(getCaseInsensitiveRegex(metricNameRegex)).map(matchedMetrics ⇒ {

      if (matchedMetrics.isEmpty)
        throw new UnsupportedOperationException(s"Unsupported query - There isnt any metric matching the regex [$metricNameRegex]")
      else if (matchedMetrics.count(m ⇒ m.mtype.equals(matchedMetrics.head.mtype)) != matchedMetrics.size)
        throw new UnsupportedOperationException(s"Unsupported query - Expression [$metricNameRegex] matches different metric types")

      matchedMetrics

    }).map {
      case metrics ⇒ {
        val functions = getSelectedFunctions(projections, metrics.head.mtype)
        InfluxCriteria(functions, metrics, filters, groupBy, limit, order)
      }
    }
  }

  def getCaseInsensitiveRegex(metricNameRegex: String) = {
    val caseInsensitiveRegex = "(?i)"
    s"$caseInsensitiveRegex$metricNameRegex"
  }

  private def getSelectedFunctions(projections: Seq[Projection], metricType: String): Seq[Field] = {
    val functionsByMetricType = allFunctionsByMetricType(metricType)

    projections.collect {
      case Field(name, alias) ⇒
        if (!functionsByMetricType.contains(name))
          throw new UnsupportedOperationException(s"$name is an invalid function for a $metricType. Valid options: [${functionsByMetricType.mkString(",")}]")
        Seq(Field(name, alias))
      case AllField() ⇒
        functionsByMetricType.collect {
          case functionName ⇒ Field(functionName, None)
        }
    }.flatten

  }

  private def allFunctionsByMetricType(metricType: String): Seq[String] = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ Functions.allHistogramFunctions
    case MetricType.Counter                  ⇒ Functions.allCounterFunctions
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }

  private def projectionParser: Parser[Seq[Projection]] =
    allFieldProjectionParser |
      rep(projectionExpressionParser).map(x ⇒ x.flatten)

  private def allFieldProjectionParser: Parser[Seq[Projection]] = "*" ^^ (_ ⇒ Seq(AllField()))

  private def projectionExpressionParser: Parser[Seq[Projection]] = {
    (knownFunctionParser | percentilesFunctionParser) ~ opt("as" ~> ident) ~ opt(",") ^^ {
      case functions ~ alias ~ _ ⇒ functions.map(f ⇒ Field(f.name, alias))
    }
  }

  private def knownFunctionParser: Parser[Seq[Functions.Function]] = {
    elem(s"Expected some function", { e ⇒ Functions.allNames.contains(e.chars.toString) }) <~ opt("(") <~ opt(ident) <~ opt(")") ^^ {
      case f ⇒ Seq(Functions.withName(f.chars.toString))
    }
  }

  private def percentilesFunctionParser: Parser[Seq[Functions.Function]] = {
    "percentiles" ~> "(" ~> rep(validPercentilesParser) <~ ")" ^^ {
      case selectedPercentiles if selectedPercentiles.nonEmpty ⇒ selectedPercentiles.collect {
        case p ⇒ Functions.percentileByValue(p)
      }
      case _ ⇒ Functions.allPercentiles
    } |
      "percentiles" ^^ {
        case _ ⇒ Functions.allPercentiles
      }
  }

  private def validPercentilesParser: Parser[Int] =
    elem(s"Expected some valid percentile [${Functions.allPercentileNames.mkString(",")}]", {
      e ⇒ e.chars.forall(_.isDigit) && Functions.allPercentilesValues.contains(e.chars.toInt)
    }) ^^
      (_.chars.toInt)

  private def tableParser: Parser[String] =
    "from" ~> stringLit ^^ {
      case metricNameRegex ⇒ metricNameRegex
    }

  private def filterParser: Parser[List[Filter]] = "where" ~> filterExpression

  private def filterExpression: Parser[List[Filter]] =
    rep(
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
        timeUnit.map {
          toMillis(number, _)
        }.getOrElse(number.toLong)
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

  def isForcedResolution(force: Option[String]) = force match {
    case Some(_) ⇒ true
    case _       ⇒ false
  }

  private def groupByParser: Parser[GroupBy] =
    opt("force") ~ "group_by_time" ~ "(" ~ timeWindowParser ~ ")" ^^ {
      case force ~ _ ~ _ ~ timeWindowDuration ~ _ ⇒ GroupBy(isForcedResolution(force), timeWindowDuration)
    }

  private def timeWindowParser: Parser[FiniteDuration] =
    (numericLit ~ opt(".") ~ opt(numericLit) ~ (TimeSuffixes.Seconds | TimeSuffixes.Minutes | TimeSuffixes.Hours)) ^^ {
      case number ~ _ ~ _ ~ timeSuffix ⇒ {
        val window = timeSuffix match {
          case TimeSuffixes.Seconds ⇒ new FiniteDuration(number.toLong, TimeUnit.SECONDS)
          case TimeSuffixes.Minutes ⇒ new FiniteDuration(number.toLong, TimeUnit.MINUTES)
          case TimeSuffixes.Hours   ⇒ new FiniteDuration(number.toLong, TimeUnit.HOURS)
        }

        window
      }
    }

  private def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def orderParser: Parser[Boolean] = "order" ~> ("asc" | "desc") ^^ { case o ⇒ "asc".equals(o) }

  private def stringParser: Parser[String] = stringLit ^^ { case s ⇒ s }

  protected def now: Long = System.currentTimeMillis()

}
