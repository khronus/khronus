package com.despegar.metrik.web.service

import org.scalatest.{ ShouldMatchers, FunSuite }
import org.scalatest.mock.MockitoSugar
import com.despegar.metrik.web.service.influx.parser.{ InfluxCriteria, InfluxQueryParser }
import org.specs2.Specification
import org.scalatest.Matchers._
import com.despegar.metrik.web.service.influx.parser._

/**
 * Created by aholman on 23/10/14.
 */
class InfluxQueryParserSpec extends FunSuite with ShouldMatchers {
  // TODO Fechas: '2013-08-12 23:32:01.232' - now() - 1h
  // TODO group by time(30s)

  val parser = new InfluxQueryParser()

  test("basic Influx query should be parsed ok") {
    val query = "select aValue from metricA"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("select * should be parsed ok") {
    val query = "select * from metricA as m"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    influxCriteria.projection.isInstanceOf[AllField] should be(true)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(Some("m"))

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("Influx query with max should be parsed ok") {
    val query = "select max(value) as maxValue from metricA"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Max)
    resultedField.alias should be(Some("maxValue"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("Influx query with avg should be parsed ok") {
    val query = "select avg(value) as avgValue from metricA"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Avg)
    resultedField.alias should be(Some("avgValue"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("Influx query with count should be parsed ok") {
    val query = "select count(value) as counter from metricA"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("count")
    resultedField.alias should be(Some("counter"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("Where clause should be parsed ok") {
    val query = "select aValue from metricA where host = 'aHost'"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val filterExpression = influxCriteria.filter.get
    (filterExpression.asInstanceOf[Eq]).leftExpression equals Identifier("host")
    (filterExpression.asInstanceOf[Eq]).rightExpression equals StringLiteral("aHost")

    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(None)
  }

  test("Group by clause should be parsed ok") {
    val query = "select aValue as counter from metricA group by minValue"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(Some("counter"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.get.keys.size should be(1)
    influxCriteria.groupBy.get.keys(0) should be(Identifier("minValue"))

    influxCriteria.limit should be(None)
    influxCriteria.filter should be(None)
  }

  test("Limit clause should be parsed ok") {
    val query = "select aValue from metricA limit 10"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.filter should be(None)
    influxCriteria.groupBy should be(None)
    influxCriteria.limit should be(Some(10))
  }

  test("Full Influx query should be parsed ok") {
    val query = "select count(value) as counter from metricA where time > 1000 and host = 'aHost' group by minValue limit 550;"
    val influxCriteriaResult = parser.parse(query)

    val influxCriteria = influxCriteriaResult.get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("count")
    resultedField.alias should be(Some("counter"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val filterExpression = influxCriteria.filter.get
    val andExpression = filterExpression.asInstanceOf[And]

    val timeFilter = andExpression.leftExpression.asInstanceOf[Gt]
    timeFilter.leftExpression equals Identifier("time")
    timeFilter.rightExpression equals IntLiteral(1000)

    val hostFilter = andExpression.rightExpression.asInstanceOf[Eq]
    hostFilter.leftExpression equals Identifier("host")
    hostFilter.rightExpression equals StringLiteral("aHost")

    influxCriteria.groupBy.get.keys.size should be(1)
    influxCriteria.groupBy.get.keys(0) should be(Identifier("minValue"))

    influxCriteria.limit should be(Some(550))
  }

  test("Query without projection should fail") {
    val query = "select from metricA"
    val influxCriteriaResult = parser.parse(query)

    influxCriteriaResult should be(None)
  }

  test("Query without from clause should fail") {
    val query = "select max(value) "
    val influxCriteriaResult = parser.parse(query)

    influxCriteriaResult should be(None)
  }

  test("Query without table should fail") {
    val query = "select max(value) from"
    val influxCriteriaResult = parser.parse(query)

    influxCriteriaResult should be(None)

  }

  test("Query with unclosed string literal should fail") {
    val query = "select max(value) from metricA where host = 'host"
    val influxCriteriaResult = parser.parse(query)

    influxCriteriaResult should be(None)

  }


}
