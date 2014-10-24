package com.despegar.metrik.web.service.influx.parser

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._

import scala.util.parsing.input.CharArrayReader.EofCh

class InfluxQueryParser extends StandardTokenParsers {

  class InfluxLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    override def token: Parser[Token] =
      (identChar ~ rep(identChar | digit) ^^ { case first ~ rest ⇒ processIdent(first :: rest mkString "") }
        | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
          case i ~ None    ⇒ NumericLit(i mkString "")
          case i ~ Some(d) ⇒ FloatLit(i.mkString("") + "." + d.mkString(""))
        }
        | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' ⇒ StringLit(chars mkString "") }
        | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' ⇒ StringLit(chars mkString "") }
        | EofCh ^^^ EOF
        | '\'' ~> failure("unclosed string literal")
        | '\"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character"))

  }
  override val lexical = new InfluxLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  val functions = Seq("count", "avg", "min", "max")

  lexical.reserved += ("select", "as", "from", "where", "or", "and", "not", "group", "by", "limit", "for", "between", "like", "null", "is", "date")

  lexical.reserved ++= functions

  lexical.delimiters += ("*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";")

  def influxQueryParser: Parser[InfluxCriteria] =
    "select" ~> projectionParser ~
      tableParser ~ opt(filterParser) ~
      opt(groupByParser) ~ opt(limitParser) <~ opt(";") ^^ {
        case projection ~ table ~ filters ~ groupBy ~ limit ⇒ InfluxCriteria(projection, table, filters, groupBy, limit)
      }

  def projectionParser: Parser[Projection] =
    "*" ^^ (_ ⇒ StarProjection()) |
      expr ~ opt("as" ~> ident) ^^ {
        case expr ~ ident ⇒ ExpressionProjection(expr, ident)
      }

  def expr: Parser[Expression] = orExpressionParser

  def orExpressionParser: Parser[Expression] =
    andExpressionParser * ("or" ^^^ { (a: Expression, b: Expression) ⇒ Or(a, b) })

  def andExpressionParser: Parser[Expression] =
    cmp_expr * ("and" ^^^ { (a: Expression, b: Expression) ⇒ And(a, b) })

  // TODO: this function is nasty- clean it up!
  def cmp_expr: Parser[Expression] =
    primaryExpressionParser ~ rep(
      ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ primaryExpressionParser ^^ {
        case op ~ rhs ⇒ (op, rhs)
      } |
        "between" ~ primaryExpressionParser ~ "and" ~ primaryExpressionParser ^^ {
          case op ~ a ~ _ ~ b ⇒ (op, a, b)
        } |
        opt("not") ~ "like" ~ primaryExpressionParser ^^ { case n ~ op ~ a ⇒ (op, a, n.isDefined) }) ^^ {
        case lhs ~ elems ⇒
          elems.foldLeft(lhs) {
            case (acc, (("=", rhs: Expression)))                   ⇒ Eq(acc, rhs)
            case (acc, (("<>", rhs: Expression)))                  ⇒ Neq(acc, rhs)
            case (acc, (("!=", rhs: Expression)))                  ⇒ Neq(acc, rhs)
            case (acc, (("<", rhs: Expression)))                   ⇒ Lt(acc, rhs)
            case (acc, (("<=", rhs: Expression)))                  ⇒ Le(acc, rhs)
            case (acc, ((">", rhs: Expression)))                   ⇒ Gt(acc, rhs)
            case (acc, ((">=", rhs: Expression)))                  ⇒ Ge(acc, rhs)
            case (acc, (("between", l: Expression, r: Expression)))   ⇒ And(Ge(acc, l), Le(acc, r))
            case (acc, (("like", e: Expression, n: Boolean)))      ⇒ Like(acc, e, n)
          }
      } |
      "not" ~> cmp_expr ^^ (Not(_))


  def primaryExpressionParser: Parser[Expression] =
    literalParser |
      knownFunctionParser |
      ident ~ opt("." ~> ident | "(" ~> repsep(expr, ",") <~ ")") ^^ {
        case id ~ None            ⇒ FieldIdentifier(None, id)
        case a ~ Some(b: String)  ⇒ FieldIdentifier(Some(a), b)
      }

  def knownFunctionParser: Parser[Expression] =
    "count" ~> "(" ~> expr <~ ")" ^^ (CountExpr(_)) |
      "min" ~> "(" ~> expr <~ ")" ^^ (Min(_)) |
      "max" ~> "(" ~> expr <~ ")" ^^ (Max(_)) |
      "avg" ~> "(" ~> expr <~ ")" ^^ (Avg(_))

  def literalParser: Parser[Expression] =
    numericLit ^^ { case i ⇒ IntLiteral(i.toInt) } |
      floatLit ^^ { case f ⇒ FloatLiteral(f.toDouble) } |
      stringLit ^^ { case s ⇒ StringLiteral(s) } |
      "null" ^^ (_ ⇒ NullLiteral()) |
      "date" ~> stringLit ^^ (DateLiteral(_))

  def tableParser: Parser[Table] =
    "from" ~> ident ~ opt("as") ~ opt(ident) ^^ {
      case ident ~ _ ~ alias ⇒ Table(ident, alias)
    }

  def filterParser: Parser[Expression] = "where" ~> expr

  def groupByParser: Parser[GroupBy] =
    "group" ~> "by" ~> rep1sep(expr, ",") ^^ {
      case k ⇒ GroupBy(k)
    }

  def limitParser: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  def parse(sql: String): Option[InfluxCriteria] = {
    phrase(influxQueryParser)(new lexical.Scanner(sql)) match {
      case Success(r, q) ⇒ Option(r)
      case x             ⇒ println(x); None
    }
  }
}
