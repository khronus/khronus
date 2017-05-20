package com.searchlight.khronus.query

import com.searchlight.khronus.api.sql.jsql.JSQLParser
import com.searchlight.khronus.model.query._
import com.searchlight.khronus.util.Test

import scala.concurrent.duration._

class JSQLParserTest extends Test {

  val parser = new JSQLParser

  private val metric1_selector = Selector("metric1", alias = Some("m1"))
  private val metric2_selector = Selector("metric2", alias = Some("m2"))
  private val metric1_selectorWithoutAlias = Selector("metric1")

  private val range = Some(TimeRange(1, 10))

  test("in clause") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 23 and m1.country in ('AR','BR') and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selector)), Seq(metric1_selector), range, Some(And(Seq(Equals(metric1_selector, "tag1", "23"), In(metric1_selector, "country", List("AR", "BR")))))))
  }

  test("simple count") {
    val sql = """select count(m1) from "metric1 m1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selector)), Seq(metric1_selector), range, None, Some(1 minute)))
  }

  test("simple count sql without alias") {
    val sql = """select count(metric1) from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selectorWithoutAlias)), Seq(metric1_selectorWithoutAlias), range, None, Some(1 minute)))
  }

  test("anonymous count") {
    val sql = """select count() from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selectorWithoutAlias)), Seq(metric1_selectorWithoutAlias), range, None, Some(1 minute)))
  }

  test("function over value keyword") {
    val sql = """select count(value) from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selectorWithoutAlias)), Seq(metric1_selectorWithoutAlias), range, None, Some(1 minute)))
  }

  test("percentiles function") {
    val sql = "select percentiles(m1, 80, 99.9, 99.999) from metric1 m1 where time > 1 and time < 10 group by time(1h)"
    val query = parser.parse(sql)
    query should equal(Query(Seq(Percentile(metric1_selector, 80), Percentile(metric1_selector, 99.9), Percentile(metric1_selector, 99.999)), Seq(metric1_selector), range, None, Some(1 hour)))
  }

  test("multiple AND predicates") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 2016 and m1.tag2 = 'AR' and m1.tag3 = 'BUE' and m1.tag4 = 'Khronus' and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selector)), Seq(metric1_selector), range,
      Some(And(List(And(List(And(List(Equals(metric1_selector, "tag1", "2016"), Equals(metric1_selector, "tag2", "AR"))),
        Equals(metric1_selector, "tag3", "BUE"))), Equals(metric1_selector, "tag4", "Khronus"))))))
  }

  test("two metrics") {
    val sql = "select count(m1), count(m2) from metric1 m1, metric2 m2 where m1.tag1 = 23 and m2.tag1 = 20 and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selector), Count(metric2_selector)), Seq(metric1_selector, metric2_selector), range, Some(And(Seq(Equals(metric1_selector, "tag1", "23"), Equals(metric2_selector, "tag1", "20"))))))
  }

  test("parse sql div") {
    val sql = "select count(m1) / count(m2) from metric1 m1, metric2 m2 where time > 1 and time < 10 and m1.tag1 = 23 and m2.tag1 = 20 "
    val query = parser.parse(sql)
    query should equal(Query(Seq(Div(Count(metric1_selector), Count(metric2_selector))), Seq(metric1_selector, metric2_selector), range, Some(And(Seq(Equals(metric1_selector, "tag1", "23"), Equals(metric2_selector, "tag1", "20"))))))
  }

  test("group by time") {
    val sql = """select count(m1) from metric1 m1 where m1.tag1 = "2016" and time > 1 and time < 10 group by time (1h) order asc"""
    val query = parser.parse(sql)
    query should equal(Query(Seq(Count(metric1_selector)), Seq(metric1_selector), range, Some(Equals(metric1_selector, "tag1", "2016")), Some(1 hour)))
  }

}
