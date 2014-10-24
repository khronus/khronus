package com.despegar.metrik.web.service.influx.parser

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._

import scala.util.parsing.input.CharArrayReader.EofCh

class InfluxQueryParser extends StandardTokenParsers {

  class SqlLexical extends StdLexical {
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
    def regex(r: Regex): Parser[String] = new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = offset // handleWhiteSpace(source, offset)
        (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
          case Some(matched) ⇒
            Success(source.subSequence(start, start + matched.end).toString,
              in.drop(start + matched.end - offset))
          case None ⇒
            Success("", in)
        }
      }
    }
  }
  override val lexical = new SqlLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  val functions = Seq("count", "sum", "avg", "min", "max")

  lexical.reserved += (
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "asc", "desc", "not", "for", "from", "between", "like",
    "year", "month", "day", "null", "is", "date", "interval", "group", "order",
    "date")

  lexical.reserved ++= functions

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";")

  def select: Parser[MetricCriteria] =
    "select" ~> projections ~
      tableParser ~ opt(filter) ~
      opt(groupBy) ~ opt(limit) <~ opt(";") ^^ {
        case p ~ r ~ f ~ g ~ l ⇒ MetricCriteria(p, r, f, g, l)
      }

  def projections: Parser[Seq[Projection]] = repsep(projection, ",")

  def projection: Parser[Projection] =
    "*" ^^ (_ ⇒ StarProjection()) |
      expr ~ opt("as" ~> ident) ^^ {
        case expr ~ ident ⇒ ExpressionProjection(expr, ident)
      }

  def expr: Parser[Expression] = or_expr

  def or_expr: Parser[Expression] =
    and_expr * ("or" ^^^ { (a: Expression, b: Expression) ⇒ Or(a, b) })

  def and_expr: Parser[Expression] =
    cmp_expr * ("and" ^^^ { (a: Expression, b: Expression) ⇒ And(a, b) })

  // TODO: this function is nasty- clean it up!
  def cmp_expr: Parser[Expression] =
    add_expr ~ rep(
      ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ add_expr ^^ {
        case op ~ rhs ⇒ (op, rhs)
      } |
        "between" ~ add_expr ~ "and" ~ add_expr ^^ {
          case op ~ a ~ _ ~ b ⇒ (op, a, b)
        } |
        opt("not") ~ "like" ~ add_expr ^^ { case n ~ op ~ a ⇒ (op, a, n.isDefined) }) ^^ {
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

  def add_expr: Parser[Expression] =
    mult_expr * (
      "+" ^^^ { (a: Expression, b: Expression) ⇒ Plus(a, b) } |
      "-" ^^^ { (a: Expression, b: Expression) ⇒ Minus(a, b) })

  def mult_expr: Parser[Expression] =
    primary_expr * (
      "*" ^^^ { (a: Expression, b: Expression) ⇒ Mult(a, b) } |
      "/" ^^^ { (a: Expression, b: Expression) ⇒ Div(a, b) })

  def primary_expr: Parser[Expression] =
    literal |
      known_function |
      ident ~ opt("." ~> ident | "(" ~> repsep(expr, ",") <~ ")") ^^ {
        case id ~ None            ⇒ FieldIdent(None, id)
        case a ~ Some(b: String)  ⇒ FieldIdent(Some(a), b)
        case a ~ Some(xs: Seq[_]) ⇒ FunctionCall(a, xs.asInstanceOf[Seq[Expression]])
      } |
      "+" ~> primary_expr ^^ (UnaryPlus(_)) |
      "-" ~> primary_expr ^^ (UnaryMinus(_))

  def known_function: Parser[Expression] =
    "count" ~> "(" ~> ("*" ^^ (_ ⇒ CountStar()) | expr ^^ { case e ⇒ CountExpr(e) }) <~ ")" |
      "min" ~> "(" ~> expr <~ ")" ^^ (Min(_)) |
      "max" ~> "(" ~> expr <~ ")" ^^ (Max(_)) |
      "sum" ~> "(" ~> expr <~ ")" ^^ { case e ⇒ Sum(e) } |
      "avg" ~> "(" ~> expr <~ ")" ^^ { case e ⇒ Avg(e) }

  def literal: Parser[Expression] =
    numericLit ^^ { case i ⇒ IntLiteral(i.toInt) } |
      floatLit ^^ { case f ⇒ FloatLiteral(f.toDouble) } |
      stringLit ^^ { case s ⇒ StringLiteral(s) } |
      "null" ^^ (_ ⇒ NullLiteral()) |
      "date" ~> stringLit ^^ (DateLiteral(_)) |
      "interval" ~> stringLit ~ ("year" ^^^ (YEAR) | "month" ^^^ (MONTH) | "day" ^^^ (DAY)) ^^ {
        case d ~ u ⇒ IntervalLiteral(d, u)
      }

  def tableParser: Parser[Table] =
    "from" ~> ident ~ opt("as") ~ opt(ident) ^^ {
      case ident ~ _ ~ alias ⇒ Table(ident, alias)
    }

  def filter: Parser[Expression] = "where" ~> expr

  def groupBy: Parser[GroupBy] =
    "group" ~> "by" ~> rep1sep(expr, ",") ^^ {
      case k ⇒ GroupBy(k)
    }

  def limit: Parser[Int] = "limit" ~> numericLit ^^ (_.toInt)

  private def stripQuotes(s: String) = s.substring(1, s.length - 1)

  def parse(sql: String): Option[MetricCriteria] = {
    phrase(select)(new lexical.Scanner(sql)) match {
      case Success(r, q) ⇒ Option(r)
      case x             ⇒ println(x); None
    }
  }
}
