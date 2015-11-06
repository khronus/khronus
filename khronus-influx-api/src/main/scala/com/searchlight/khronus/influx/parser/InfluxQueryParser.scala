/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.searchlight.khronus.influx.parser

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import com.searchlight.khronus.model.{ Metric, MetricType, Functions }
import com.searchlight.khronus.store.{ MetaSupport, MetaStore }
import com.searchlight.khronus.util.log.Logging
import scala.concurrent.{ Future, ExecutionContext }
import com.searchlight.khronus.util.ConcurrencySupport
import com.searchlight.khronus.influx.parser.MathOperators.MathOperator

class InfluxQueryParser extends StandardTokenParsers with Logging with InfluxCriteriaBuilder with ConcurrencySupport {

  class InfluxLexical extends StdLexical

  val Separator = ","
  override val lexical = new InfluxLexical

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "group_by_time", "fill", "scale", "limit", "between", "null", "date", "time", "now", "order", "asc", "desc", "percentiles", "force",
    TimeSuffixes.Milliseconds, TimeSuffixes.Seconds, TimeSuffixes.Minutes, TimeSuffixes.Hours, TimeSuffixes.Days, TimeSuffixes.Weeks)

  lexical.reserved ++= Functions.allNames

  lexical.delimiters += ("*", Operators.Lt, Operators.Eq, Operators.Neq, Operators.Lte, Operators.Gte, Operators.Gt, Separator, "(", ")", ".", ";")
  lexical.delimiters ++= MathOperators.allSymbols

  def parse(influxQuery: String): Future[InfluxCriteria] = {
    log.debug(s"Parsing influx query [$influxQuery]")

    // TODO - Hack because of conflict: group by time & time as identifier
    val queryToParse = influxQuery.replace("group by time", "group_by_time")

    phrase(influxQueryParser)(new lexical.Scanner(queryToParse)) match {
      case Success(r, q) ⇒ r
      case x             ⇒ log.error(s"Error parsing query [$influxQuery]: $x"); throw new UnsupportedOperationException(s"Unsupported query [$influxQuery]: $x")
    }
  }

  private def influxQueryParser: Parser[Future[InfluxCriteria]] =
    "select" ~> projectionParser ~ "from" ~ tableParser ~ opt(filterParser) ~
      groupByParser ~ opt(fillerParser) ~ opt(scaleParser) ~ opt(limitParser) ~ opt(orderParser) <~ opt(";") ^^ {
        case projections ~ _ ~ tables ~ filters ~ groupBy ~ fill ~ scale ~ limit ~ order ⇒
          buildInfluxCriteria(tables, projections, filters.getOrElse(Nil), groupBy, fill, scale, order.getOrElse(true), limit.getOrElse(Int.MaxValue))
      }

  private def projectionParser: Parser[Seq[Projection]] =
    allFieldProjectionParser |
      rep(projectionExpressionParser).map(x ⇒ x.flatten)

  private def allFieldProjectionParser: Parser[Seq[Projection]] = opt(ident <~ ".") ~ "*" ^^ {
    case tableAlias ~ _ ⇒ Seq(AllField(tableAlias))
  }

  private def projectionExpressionParser: Parser[Seq[Projection]] = {
    opt(ident <~ ".") ~ percentilesFunctionParser <~ opt(Separator) ^^ {
      case tableAlias ~ functions ⇒ functions.map(f ⇒ Field(f.name, None, tableAlias))
    } |
      (operationExpressionParser | simpleFunctionExpressionParser | scalarExpressionParser) <~ opt(Separator) ^^ {
        case projection ⇒ Seq(projection)
      }
  }

  private def simpleFunctionExpressionParser: Parser[SimpleProjection] = {
    opt(ident <~ ".") ~ knownFunctionParser ~ opt("as" ~> ident) ^^ {
      case tableAlias ~ function ~ alias ⇒ Field(function.name, alias, tableAlias)
    }
  }

  private def scalarExpressionParser: Parser[SimpleProjection] = {
    doubleParser ~ "as" ~ ident ^^ {
      case doubleNumber ~ _ ~ alias ⇒ Number(doubleNumber, Some(alias))
    }
  }

  private def operationExpressionParser: Parser[Operation] = {
    operandParser ~ operatorExpressionParser ~ operandParser ~ "as" ~ ident ^^ {
      case left ~ mathOperator ~ right ~ _ ~ alias ⇒ Operation(left, right, mathOperator, alias)
    }
  }

  private def operandParser: Parser[SimpleProjection] = {
    ident ~ "." ~ knownFunctionParser ^^ {
      case tableAlias ~ _ ~ function ⇒ Field(function.name, None, Some(tableAlias))
    } |
      doubleParser ^^ {
        case doubleNumber ⇒ Number(doubleNumber)
      }
  }

  private def operatorExpressionParser: Parser[MathOperator] = {
    elem(s"Some valid operator [${MathOperators.allSymbols.mkString(",")}]", {
      e ⇒ MathOperators.allSymbols.contains(e.chars)
    }) ^^ {
      case s ⇒ MathOperators.getBySymbol(s.chars)
    }
  }

  private def knownFunctionParser: Parser[Functions.Function] = {
    elem(s"Some function", {
      e ⇒ Functions.allNames.contains(e.chars.toString)
    }) <~ opt("(") <~ opt(ident) <~ opt(")") ^^ {
      case f ⇒ Functions.withName(f.chars.toString)
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
    elem(s"Some valid percentile [${Functions.allPercentileNames.mkString(",")}]", {
      e ⇒ e.chars.forall(_.isDigit) && Functions.allPercentilesValues.contains(e.chars.toInt)
    }) ^^
      (_.chars.toInt)

  private def tableParser: Parser[Seq[Table]] =
    rep(stringLit ~ opt("as" ~> ident) <~ opt(Separator) ^^ {
      case metricNameRegex ~ aliasTable ⇒ Table(metricNameRegex, aliasTable)
    })

  private def filterParser: Parser[Seq[Filter]] = "where" ~> filterExpression

  private def filterExpression: Parser[Seq[Filter]] =
    rep(
      stringComparatorExpression |
        timestampComparatorExpression |
        timeBetweenExpression |
        relativeTimeExpression).map(x ⇒ x.flatten)

  def stringComparatorExpression: Parser[Seq[StringFilter]] = {
    ident ~ (Operators.Eq | Operators.Neq) ~ stringParser <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ strValue ⇒ List(StringFilter(identifier, operator, strValue))
    }
  }

  def timestampComparatorExpression: Parser[Seq[TimeFilter]] = {
    "time" ~ (Operators.Lt | Operators.Lte | Operators.Gt | Operators.Gte) ~ timeWithSuffixToMillisParser <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ timeInMillis ⇒ List(TimeFilter(identifier, operator, timeInMillis))
    }
  }

  def timeBetweenExpression: Parser[Seq[TimeFilter]] = {
    "time" ~ "between" ~ timeWithSuffixToMillisParser ~ "and" ~ timeWithSuffixToMillisParser <~ opt(Operators.And) ^^ {
      case identifier ~ _ ~ millisA ~ _ ~ millisB ⇒ List(TimeFilter(identifier, Operators.Gte, millisA), TimeFilter(identifier, Operators.Lte, millisB))
    }
  }

  def relativeTimeExpression: Parser[Seq[TimeFilter]] = {
    "time" ~ (Operators.Lt | Operators.Lte | Operators.Gt | Operators.Gte) ~ "now" ~ "(" ~ ")" ~ opt("-") ~ opt(timeWithSuffixToMillisParser) <~ opt(Operators.And) ^^ {
      case identifier ~ operator ~ _ ~ _ ~ _ ~ _ ~ timeInMillis ⇒
        List(TimeFilter(identifier, operator, now - timeInMillis.getOrElse(0L)))
    }
  }

  private def timeWithSuffixToMillisParser: Parser[Long] = {
    numericLit ~ opt(TimeSuffixes.Seconds | TimeSuffixes.Minutes | TimeSuffixes.Hours | TimeSuffixes.Days | TimeSuffixes.Weeks) ^^ {
      case number ~ timeUnit ⇒
        timeUnit.fold(number.toLong)(toMillis(number, _))
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
    opt("force") ~ "group_by_time" ~ "(" ~ timeWindowParser ~ ")" ^^ {
      case force ~ _ ~ _ ~ timeWindowDuration ~ _ ⇒ GroupBy(isForcedResolution(force), timeWindowDuration)
    }

  def isForcedResolution(force: Option[String]) = force match {
    case Some(_) ⇒ true
    case _       ⇒ false
  }

  private def timeWindowParser: Parser[FiniteDuration] =
    (numericLit ~ opt(".") ~ opt(numericLit) ~ (TimeSuffixes.Milliseconds | TimeSuffixes.Seconds | TimeSuffixes.Minutes | TimeSuffixes.Hours)) ^^ {
      case number ~ _ ~ _ ~ timeSuffix ⇒ {
        val window = timeSuffix match {
          case TimeSuffixes.Milliseconds ⇒ new FiniteDuration(number.toLong, TimeUnit.MILLISECONDS)
          case TimeSuffixes.Seconds      ⇒ new FiniteDuration(number.toLong, TimeUnit.SECONDS)
          case TimeSuffixes.Minutes      ⇒ new FiniteDuration(number.toLong, TimeUnit.MINUTES)
          case TimeSuffixes.Hours        ⇒ new FiniteDuration(number.toLong, TimeUnit.HOURS)
        }

        window
      }
    }

  private def fillerParser: Parser[Double] = "fill" ~> "(" ~> doubleParser <~ ")" ^^ {
    case fillNumber ⇒ fillNumber
  }

  private def scaleParser: Parser[Double] = "scale" ~> "(" ~> doubleParser <~ ")" ^^ {
    case scaleNumber ⇒ scaleNumber
  }

  private def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def orderParser: Parser[Boolean] = "order" ~> ("asc" | "desc") ^^ {
    case o ⇒ "asc".equals(o)
  }

  private def stringParser: Parser[String] = stringLit ^^ {
    case s ⇒ s
  }

  private def doubleParser: Parser[Double] = {
    opt("-") ~ numericLit ~ opt("." ~> numericLit) ^^ {
      case negativeSign ~ firstPart ~ secondPart ⇒ {
        val sign = if (negativeSign.isDefined) "-" else ""
        val decimals = if (secondPart.isDefined) s".${secondPart.get}" else ""
        s"$sign$firstPart$decimals".toDouble
      }
    }
  }

  protected def now: Long = System.currentTimeMillis()

}
