package com.despegar.metrik.web.service.influx.parser

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._

import scala.util.parsing.input.CharArrayReader.EofCh
import com.despegar.metrik.util.Logging

class InfluxQueryParser extends StandardTokenParsers with Logging {

  class InfluxLexical extends StdLexical

  override val lexical = new InfluxLexical

  val functions = Seq(Functions.Count, Functions.Avg, Functions.Min, Functions.Max)

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "group", "by", "limit", "between", "null", "date")

  lexical.reserved ++= functions

  lexical.delimiters += ("*", Operators.Lt, Operators.Eq, Operators.Neq, Operators.Lte, Operators.Gte, Operators.Gt, "(", ")", ",", ".", ";")


  def parse(influxQuery: String): Option[InfluxCriteria] = {
    log.info(s"Parsing influx query [$influxQuery]")
    phrase(influxQueryParser)(new lexical.Scanner(influxQuery)) match {
      case Success(r, q) ⇒ Option(r)
      case x             ⇒ log.error(s"Error parsing query [$influxQuery]: $x"); None
    }
  }


  private def influxQueryParser: Parser[InfluxCriteria] =
    "select" ~> projectionParser ~
      tableParser ~ opt(filterParser) ~
      opt(groupByParser) ~ opt(limitParser) <~ opt(";") ^^ {
        case projection ~ table ~ filters ~ groupBy ~ limit ⇒ InfluxCriteria(projection, table, filters, groupBy, limit)
      }


  private def projectionParser: Parser[Projection] =
    "*" ^^ (_ ⇒ AllField()) |
      projectionExpressionParser ~ opt("as" ~> ident) ^^ {
        case x ~ alias ⇒ {
          x match {
            case id: Identifier       ⇒ Field(id.value, alias)
            case proj: ProjectionExpression ⇒ Field(proj.function, alias)
          }
        }
      }

  private def projectionExpressionParser: Parser[Expression] =
    ident ^^ (Identifier(_))|
      knownFunctionParser

  private def knownFunctionParser: Parser[Expression] =
    Functions.Count ~> "(" ~> ident <~ ")" ^^ (Count(_)) |
      Functions.Min ~> "(" ~> ident <~ ")" ^^ (Min(_)) |
      Functions.Max ~> "(" ~> ident <~ ")" ^^ (Max(_)) |
      Functions.Avg ~> "(" ~> ident <~ ")" ^^ (Avg(_))


  private def tableParser: Parser[Table] =
    "from" ~> ident ~ opt("as") ~ opt(ident) ^^ {
      case ident ~ _ ~ alias ⇒ Table(ident, alias)
    }


  private def filterParser: Parser[Expression] = "where" ~> filterExpression

  private def filterExpression: Parser[Expression] = orExpressionParser

  private def orExpressionParser: Parser[Expression] =
    andExpressionParser * (Operators.Or ^^^ { (left: Expression, right: Expression) ⇒ Or(left, right) })

  private def andExpressionParser: Parser[Expression] =
    comparatorExpression * (Operators.And ^^^ { (left: Expression, right: Expression) ⇒ And(left, right) })

  // TODO: this function is nasty- clean it up!
  private def comparatorExpression: Parser[Expression] =
    primaryExpressionParser ~ rep(
      (Operators.Eq | Operators.Neq | Operators.Lt | Operators.Lte | Operators.Gt | Operators.Gte) ~ primaryExpressionParser ^^ {
        case operator ~ rhs ⇒ (operator, rhs)
      } |
      "between" ~ primaryExpressionParser ~ "and" ~ primaryExpressionParser ^^ {
        case operator ~ a ~ _ ~ b ⇒ (operator, a, b)
      }) ^^ {
        case lhs ~ elems ⇒
          elems.foldLeft(lhs) {
            case (acc, ((Operators.Eq, rhs: Expression)))           ⇒ Eq(acc, rhs)
            case (acc, ((Operators.Neq, rhs: Expression)))          ⇒ Neq(acc, rhs)
            case (acc, ((Operators.Lt, rhs: Expression)))           ⇒ Lt(acc, rhs)
            case (acc, ((Operators.Lte, rhs: Expression)))          ⇒ Le(acc, rhs)
            case (acc, ((Operators.Gt, rhs: Expression)))           ⇒ Gt(acc, rhs)
            case (acc, ((Operators.Gte, rhs: Expression)))          ⇒ Ge(acc, rhs)
            case (acc, (("between", l: Expression, r: Expression))) ⇒ And(Ge(acc, l), Le(acc, r))
          }
      }

  private def primaryExpressionParser: Parser[Expression] =
    literalParser |
      knownFunctionParser |
      ident ^^ (Identifier(_))

  private def literalParser: Parser[Expression] =
    numericLit ^^ { case i ⇒ IntLiteral(i.toInt) } |
      stringLit ^^ { case s ⇒ StringLiteral(s) } |
      "null" ^^ (_ ⇒ NullLiteral()) |
      "date" ~> stringLit ^^ (DateLiteral(_))


  private def groupByParser: Parser[GroupBy] =
    "group" ~> "by" ~> rep1sep(filterExpression, ",") ^^ {
      case k ⇒ GroupBy(k)
    }

  private def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

}
