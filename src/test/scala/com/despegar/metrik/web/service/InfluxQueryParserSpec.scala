package com.despegar.metrik.web.service

import org.scalatest.{ShouldMatchers, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.despegar.metrik.web.service.influx.parser.{InfluxCriteria, InfluxQueryParser}
import org.specs2.Specification
import org.scalatest.Matchers._


/**
 * Created by aholman on 23/10/14.
 */
class InfluxQueryParserSpec extends FunSuite with ShouldMatchers {
  // TODO Fechas: '2013-08-12 23:32:01.232' - now() - 1h
  // TODO group by time(30s)

  test("basic Influx query should be parsed ok") {
    val query = "select aValue from metricA"
    val parser = new InfluxQueryParser()
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    println(influxCriteria)
    println(influxCriteria.toSqlString)
  }

  test("Influx query with max should be parsed ok") {
    val query = "select max(value) as maxValue from metricA"
    val parser = new InfluxQueryParser()
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    println(influxCriteria)
    println(influxCriteria.toSqlString)
  }

  test("Influx query with avg should be parsed ok") {
    val query = "select avg(value) as avgValue from metricA"
    val parser = new InfluxQueryParser()
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    println(influxCriteria)
    println(influxCriteria.toSqlString)
  }

  test("Full Influx query should be parsed ok") {
    val query = "select count(value) as counter from metricA where time > 1000 and time < 5000 and host = 'aHost' group by minValue limit 550;"
    val parser = new InfluxQueryParser()
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    println(influxCriteria)
    println(influxCriteria.toSqlString)
  }





}
