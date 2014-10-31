package com.despegar.metrik.web.service

import com.despegar.metrik.model.Functions
import org.scalatest.{ ShouldMatchers, FunSuite }
import org.scalatest.mock.MockitoSugar
import com.despegar.metrik.web.service.influx.parser.{ InfluxCriteria, InfluxQueryParser }
import org.specs2.Specification
import org.scalatest.Matchers._
import com.despegar.metrik.web.service.influx.parser._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
 * Created by aholman on 23/10/14.
 */
class InfluxQueryParserSpec extends FunSuite with ShouldMatchers {
  // TODO - Where con soporte para expresiones regulares: =~ matches against, !~ doesn’t match against

  val parser = new InfluxQueryParser()

  test("basic Influx query should be parsed ok") {
    val query = "select aValue from metricA group by time(1h)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("select * should be parsed ok") {
    val query = "select * from metricA as a group by time (30s)"
    val influxCriteria = parser.parse(query).get

    influxCriteria.projection.isInstanceOf[AllField] should be(true)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(Some("a"))

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("Influx query with max should be parsed ok") {
    val query = "select max(value) as maxValue from metricA group by time(1m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Max.value)
    resultedField.alias should be(Some("maxValue"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("Influx query with min should be parsed ok") {
    val query = "select min(value) from metricA group by time(5m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Min.value)
  }

  test("Influx query with avg should be parsed ok") {
    val query = "select avg(value) from metricA group by time(30s)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Avg.value)
  }

  test("Influx query with count should be parsed ok") {
    val query = "select count(value) from metricA group by time(1m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be(Functions.Count.value)
  }

  test("Influx query with percentiles should be parsed ok") {
    val query50 = "select p50(value) from metricA group by time(30s)"
    val resultedField50 = parser.parse(query50).get.projection.asInstanceOf[Field]
    resultedField50.name should be(Functions.Percentile50.value)

    val query80 = "select p80(value) from metricA group by time(30s)"
    val resultedField80 = parser.parse(query80).get.projection.asInstanceOf[Field]
    resultedField80.name should be(Functions.Percentile80.value)

    val query90 = "select p90(value) from metricA group by time(30s)"
    val resultedField90 = parser.parse(query90).get.projection.asInstanceOf[Field]
    resultedField90.name should be(Functions.Percentile90.value)

    val query95 = "select p95(value) from metricA group by time(30s)"
    val resultedField95 = parser.parse(query95).get.projection.asInstanceOf[Field]
    resultedField95.name should be(Functions.Percentile95.value)

    val query99 = "select p99(value) from metricA group by time(30s)"
    val resultedField99 = parser.parse(query99).get.projection.asInstanceOf[Field]
    resultedField99.name should be(Functions.Percentile99.value)

    val query999 = "select p999(value) from metricA group by time(30s)"
    val resultedField999 = parser.parse(query999).get.projection.asInstanceOf[Field]
    resultedField999.name should be(Functions.Percentile999.value)

  }

  test("Where clause should be parsed ok") {
    val query = "select aValue from metricA where host = 'aHost' group by time(5m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val stringFilter = influxCriteria.filters(0).asInstanceOf[StringFilter]
    stringFilter.identifier should be("host")
    stringFilter.operator should be(Operators.Eq)
    stringFilter.value should be("aHost")

    influxCriteria.groupBy.duration.length should be(5)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(None)
  }

  test("Where clause with and should be parsed ok") {
    val query = "select aValue from metricA where time >= 1414508614 and time < 1414509500 group by time(10m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lt)
    filter2.value should be(1414509500L)

    influxCriteria.groupBy.duration.length should be(10)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(None)
  }

  test("Where clause with time suffix should be parsed ok") {
    val query = "select aValue from metricA where time >= 1414508614s group by time(10m)"
    val influxCriteria = parser.parse(query).get

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614000L)
  }

  test("Where clauses like (now - 1h) should be parsed ok") {
    val mockedNow = 1414767928000L
    val mockedParser = new InfluxQueryParser() {
      override def now: Long = mockedNow
    }

    val criteriaNow = mockedParser.parse("select aValue from metricA where time > now() group by time(10m)").get
    val filterNow = criteriaNow.filters(0).asInstanceOf[TimeFilter]
    filterNow.identifier should be("time")
    filterNow.operator should be(Operators.Gt)
    filterNow.value should be(mockedNow)

    val criteriaNow20s = mockedParser.parse("select aValue from metricA where time < now() - 20s group by time(10m)").get
    val filterNow20s = criteriaNow20s.filters(0).asInstanceOf[TimeFilter]
    filterNow20s.identifier should be("time")
    filterNow20s.operator should be(Operators.Lt)
    filterNow20s.value should be(mockedNow - TimeUnit.SECONDS.toMillis(20))

    val criteriaNow5m = mockedParser.parse("select aValue from metricA where time <= now() - 5m group by time(10m)").get
    val filterNow5m = criteriaNow5m.filters(0).asInstanceOf[TimeFilter]
    filterNow5m.identifier should be("time")
    filterNow5m.operator should be(Operators.Lte)
    filterNow5m.value should be(mockedNow - TimeUnit.MINUTES.toMillis(5))

    val criteriaNow3h = mockedParser.parse("select aValue from metricA where time >= now() - 3h group by time(10m)").get
    val filterNow3h = criteriaNow3h.filters(0).asInstanceOf[TimeFilter]
    filterNow3h.identifier should be("time")
    filterNow3h.operator should be(Operators.Gte)
    filterNow3h.value should be(mockedNow - TimeUnit.HOURS.toMillis(3))

    val criteriaNow10d = mockedParser.parse("select aValue from metricA where time >= now() - 10d group by time(10m)").get
    val filterNow10d = criteriaNow10d.filters(0).asInstanceOf[TimeFilter]
    filterNow10d.identifier should be("time")
    filterNow10d.operator should be(Operators.Gte)
    filterNow10d.value should be(mockedNow - TimeUnit.DAYS.toMillis(10))

    val criteriaNow2w = mockedParser.parse("select aValue from metricA where time <= now() - 2w group by time(10m)").get
    val filterNow2w = criteriaNow2w.filters(0).asInstanceOf[TimeFilter]
    filterNow2w.identifier should be("time")
    filterNow2w.operator should be(Operators.Lte)
    filterNow2w.value should be(mockedNow - TimeUnit.DAYS.toMillis(14))
  }

  test("Between clause should be parsed ok") {
    val query = "select aValue from metricA where time between 1414508614 and 1414509500s group by time(30m)"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lte)
    filter2.value should be(1414509500000L)

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(None)
  }

  test("Group by clause by valid windows should be parsed ok") {
    val influxCriteriaResult30s = parser.parse("select aValue as counter from metricA group by time(30s)")
    influxCriteriaResult30s.get.groupBy.duration.length should be(30)
    influxCriteriaResult30s.get.groupBy.duration.unit should be(TimeUnit.SECONDS)

    val influxCriteriaResult1m = parser.parse("select aValue as counter from metricA group by time(1m)")
    influxCriteriaResult1m.get.groupBy.duration.length should be(1)
    influxCriteriaResult1m.get.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult5m = parser.parse("select aValue as counter from metricA group by time(5m)")
    influxCriteriaResult5m.get.groupBy.duration.length should be(5)
    influxCriteriaResult5m.get.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult10m = parser.parse("select aValue as counter from metricA group by time(10m)")
    influxCriteriaResult10m.get.groupBy.duration.length should be(10)
    influxCriteriaResult10m.get.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult30m = parser.parse("select aValue as counter from metricA group by time(30m)")
    influxCriteriaResult30m.get.groupBy.duration.length should be(30)
    influxCriteriaResult30m.get.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult1h = parser.parse("select aValue as counter from metricA group by time(1h)")
    influxCriteriaResult1h.get.groupBy.duration.length should be(1)
    influxCriteriaResult1h.get.groupBy.duration.unit should be(TimeUnit.HOURS)

  }

  test("Limit clause should be parsed ok") {
    val query = "select aValue from metricA group by time(1m) limit 10"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Some(10))
  }

  test("Full Influx query should be parsed ok") {
    val query = "select count(value) as counter from metricA where time > 1000 and time <= 5000 and host <> 'aHost' group by time(30s) limit 550;"
    val influxCriteria = parser.parse(query).get

    val resultedField = influxCriteria.projection.asInstanceOf[Field]
    resultedField.name should be("count")
    resultedField.alias should be(Some("counter"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gt)
    filter1.value should be(1000L)

    val filter2 = influxCriteria.filters(1).asInstanceOf[TimeFilter]
    filter2.identifier should be("time")
    filter2.operator should be(Operators.Lte)
    filter2.value should be(5000L)

    val filter3 = influxCriteria.filters(2).asInstanceOf[StringFilter]
    filter3.identifier should be("host")
    filter3.operator should be(Operators.Neq)
    filter3.value should be("aHost")

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.limit should be(Some(550))
  }

  test("Query without projection should fail") {
    val query = "select from metricA"
    parser.parse(query) should be(None)
  }

  test("Query without from clause should fail") {
    val query = "select max(value) "
    parser.parse(query) should be(None)
  }

  test("Query without table should fail") {
    val query = "select max(value) from"
    parser.parse(query) should be(None)
  }

  test("Query with unclosed string literal should fail") {
    val query = "select max(value) from metricA where host = 'host"
    parser.parse(query) should be(None)
  }

  test("Query with unclosed parenthesis should fail") {
    val query = "select max(value) from metricA group by time(30s"
    parser.parse(query) should be(None)
  }

  test("Query with invalid time window should fail") {
    val query = "select max(value) from metricA group by time(3s)"
    parser.parse(query) should be(None)
  }

  test("Query with invalid time now expression should fail") {
    val query = "select max(value) from metricA where time  > now() - 1j group by time(30s)"
    parser.parse(query) should be(None)
  }

}