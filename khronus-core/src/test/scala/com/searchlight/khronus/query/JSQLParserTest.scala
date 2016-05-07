package com.searchlight.khronus.query

import com.searchlight.khronus.query.projection.{ Count, Div, Percentiles }
import com.searchlight.khronus.util.Test

import scala.concurrent.duration._

class JSQLParserTest extends Test {

  val parser = new JSQLParser

  test("parse sql with IN clause") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 23 and m1.country in ('AR','BR') and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), Some(And(Equals("m1", "tag1", "23"), In("m1", "country", List("AR", "BR")))), Slice(1, 10)))
  }

  test("parse simple count sql") {
    val sql = """select count(m1) from "metric1 m1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), None, Slice(1, 10), Some(1 minute)))
  }

  test("parse simple count sql without alias") {
    val sql = """select count(metric1) from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("metric1")), Seq(QMetric("metric1", "metric1")), None, Slice(1, 10), Some(1 minute)))
  }

  test("parse simple anonymous count sql without alias") {
    val sql = """select count() from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count()), Seq(QMetric("metric1", "metric1")), None, Slice(1, 10), Some(1 minute)))
  }

  test("parse simple count value sql without alias") {
    val sql = """select count(value) from "metric1" where time > 1 and time < 10 group by time (1m)"""
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count()), Seq(QMetric("metric1", "metric1")), None, Slice(1, 10), Some(1 minute)))
  }

  test("parse simple percentiles sql") {
    val sql = "select percentiles(m1, 80, 99.9, 99.999) from metric1 m1 where time > 1 and time < 10 group by time(1h)"
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Percentiles("m1", Seq(80, 99.9, 99.999))), Seq(QMetric("metric1", "m1")), None, Slice(1, 10), Some(1 hour)))
  }

  test("parse sql with multiples AND predicates") {
    val sql = "select count(m1) from metric1 m1 where m1.tag1 = 2016 and m1.tag2 = 'AR' and m1.tag3 = 'BUE' and m1.tag4 = 'Khronus' and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), Some(And(And(And(Equals("m1", "tag1", "2016"), Equals("m1", "tag2", "AR")), Equals("m1", "tag3", "BUE")), Equals("m1", "tag4", "Khronus"))), Slice(1, 10)))
  }

  test("parse sql with two metrics") {
    val sql = "select count(m1), count(m2) from metric1 m1, metric2 m2 where m1.tag1 = 23 and m2.tag1 = 20 and time > 1 and time < 10 "
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1"), Count("m2")), Seq(QMetric("metric1", "m1"), QMetric("metric2", "m2")), Some(And(Equals("m1", "tag1", "23"), Equals("m2", "tag1", "20"))), Slice(1, 10)))
  }

  test("parse sql div") {
    val sql = "select count(m1) / count(m2) from metric1 m1, metric2 m2 where time > 1 and time < 10 and m1.tag1 = 23 and m2.tag1 = 20 "
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Div(Count("m1"), Count("m2"))), Seq(QMetric("metric1", "m1"), QMetric("metric2", "m2")), Some(And(Equals("m1", "tag1", "23"), Equals("m2", "tag1", "20"))), Slice(1, 10)))
  }

  test("parse sql with group by time") {
    val sql = """select count(m1) from metric1 m1 where m1.tag1 = "2016" and time > 1 and time < 10 group by time (1h) order asc"""
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), Some(Equals("m1", "tag1", "2016")), Slice(1, 10), Some(1 hour)))
  }

  test("parse simple count sql with comment") {
    val sql = "/*dynamic*/ select count(m1) from metric1 m1 where time > 1 and time < 10 group by time(1h)"
    val query = parser.parse(sql)
    query should equal(DynamicQuery(Seq(Count("m1")), Seq(QMetric("metric1", "m1")), None, Slice(1, 10), Some(1 hour)))
  }

}
