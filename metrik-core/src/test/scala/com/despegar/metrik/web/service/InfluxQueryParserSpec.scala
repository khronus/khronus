package com.despegar.metrik.web.service

import com.despegar.metrik.model.Functions
import org.scalatest.{ ShouldMatchers, FunSuite }
import org.scalatest.mock.MockitoSugar
import com.despegar.metrik.web.service.influx.parser.{ InfluxCriteria, InfluxQueryParser }
import org.specs2.Specification
import org.scalatest.Matchers._
import com.despegar.metrik.web.service.influx.parser._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.despegar.metrik.web.service.influx.parser.TimeFilter
import com.despegar.metrik.web.service.influx.parser.StringFilter
import scala.Some
import com.despegar.metrik.web.service.influx.parser.AllField
import com.despegar.metrik.web.service.influx.parser.Field

/**
 * Created by aholman on 23/10/14.
 */
class InfluxQueryParserSpec extends FunSuite with ShouldMatchers {
  // TODO - Where con soporte para expresiones regulares: =~ matches against, !~ doesn’t match against

  val parser = new InfluxQueryParser() {
    override def getConfiguredWindows: Seq[FiniteDuration] = {
      Seq(FiniteDuration(30, TimeUnit.SECONDS),
        FiniteDuration(1, TimeUnit.MINUTES),
        FiniteDuration(5, TimeUnit.MINUTES),
        FiniteDuration(2, TimeUnit.HOURS))
    }
  }

  test("basic Influx query should be parsed ok") {
    val query = "select aValue from \"metric:A\\12:3\" group by time(2h)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metric:A\\12:3")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("select with many projections should be parsed ok") {
    val query = "select aValue max(value) as maxValue min(value) from \"metricA\" group by time(2h)"
    val influxCriteria = parser.parse(query)

    influxCriteria.projections.size should be(3)

    val firstProjection = influxCriteria.projections(0).asInstanceOf[Field]
    firstProjection.name should be("aValue")
    firstProjection.alias should be(None)

    val secondProjection = influxCriteria.projections(1).asInstanceOf[Field]
    secondProjection.name should be(Functions.Max.name)
    secondProjection.alias should be(Some("maxValue"))

    val thirdProjection = influxCriteria.projections(2).asInstanceOf[Field]
    thirdProjection.name should be(Functions.Min.name)
    thirdProjection.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("select * should be parsed ok") {
    val query = "select * from \"metricA\" as a group by time (30s)"
    val influxCriteria = parser.parse(query)

    influxCriteria.projections.size should be(1)
    influxCriteria.projections(0).isInstanceOf[AllField] should be(true)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(Some("a"))

    influxCriteria.groupBy.duration.length should be(30)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.SECONDS)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("Influx query with max should be parsed ok") {
    val query = "select max(value) as maxValue from \"metricA\" group by time(1m)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be(Functions.Max.name)
    resultedField.alias should be(Some("maxValue"))

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(None)
  }

  test("Influx query with min should be parsed ok") {
    val query = "select min(value) from \"metricA\" group by time(5m)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be(Functions.Min.name)
  }

  test("Influx query with avg should be parsed ok") {
    val query = "select avg(value) from \"metricA\" group by time(30s)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be(Functions.Avg.name)
  }

  test("Influx query with count should be parsed ok") {
    val query = "select count(value) from \"metricA\" group by time(1m)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be(Functions.Count.name)
  }

  test("Influx query with percentiles should be parsed ok") {
    val query50 = "select p50(value) from \"metricA\" group by time(30s)"
    val resultedField50 = parser.parse(query50).projections(0).asInstanceOf[Field]
    resultedField50.name should be(Functions.Percentile50.name)

    val query80 = "select p80(value) from \"metricA\" group by time(30s)"
    val resultedField80 = parser.parse(query80).projections(0).asInstanceOf[Field]
    resultedField80.name should be(Functions.Percentile80.name)

    val query90 = "select p90(value) from \"metricA\" group by time(30s)"
    val resultedField90 = parser.parse(query90).projections(0).asInstanceOf[Field]
    resultedField90.name should be(Functions.Percentile90.name)

    val query95 = "select p95(value) from \"metricA\" group by time(30s)"
    val resultedField95 = parser.parse(query95).projections(0).asInstanceOf[Field]
    resultedField95.name should be(Functions.Percentile95.name)

    val query99 = "select p99(value) from \"metricA\" group by time(30s)"
    val resultedField99 = parser.parse(query99).projections(0).asInstanceOf[Field]
    resultedField99.name should be(Functions.Percentile99.name)

    val query999 = "select p999(value) from \"metricA\" group by time(30s)"
    val resultedField999 = parser.parse(query999).projections(0).asInstanceOf[Field]
    resultedField999.name should be(Functions.Percentile999.name)

  }

  test("All Percentiles function should be parsed ok") {
    val queryAllPercentiles = "select percentiles() from \"metricA\" group by time(30s)"
    val projections = parser.parse(queryAllPercentiles).projections

    projections.size should be(6)

    projections(0).asInstanceOf[Field].name should be(Functions.Percentile50.name)
    projections(1).asInstanceOf[Field].name should be(Functions.Percentile80.name)
    projections(2).asInstanceOf[Field].name should be(Functions.Percentile90.name)
    projections(3).asInstanceOf[Field].name should be(Functions.Percentile95.name)
    projections(4).asInstanceOf[Field].name should be(Functions.Percentile99.name)
    projections(5).asInstanceOf[Field].name should be(Functions.Percentile999.name)
  }

  test("Some Percentiles function should be parsed ok") {
    val queryPercentiles = "select percentiles(80 99 50) from \"metricA\" group by time(30s)"
    val projections = parser.parse(queryPercentiles).projections

    projections.size should be(3)

    projections(0).asInstanceOf[Field].name should be(Functions.Percentile80.name)
    projections(1).asInstanceOf[Field].name should be(Functions.Percentile99.name)
    projections(2).asInstanceOf[Field].name should be(Functions.Percentile50.name)
  }

  test("Where clause should be parsed ok") {
    val query = "select aValue from \"metricA\" where host = 'aHost' group by time(5m)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
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
    val query = "select aValue from \"metricA\" where time >= 1414508614 and time < 1414509500 group by time(5m)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
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

    influxCriteria.groupBy.duration.length should be(5)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.limit should be(None)
  }

  test("Where clause with time suffix should be parsed ok") {
    val query = "select aValue from \"metricA\" where time >= 1414508614s group by time(30s)"
    val influxCriteria = parser.parse(query)

    val filter1 = influxCriteria.filters(0).asInstanceOf[TimeFilter]
    filter1.identifier should be("time")
    filter1.operator should be(Operators.Gte)
    filter1.value should be(1414508614000L)
  }

  test("Where clauses like (now - 1h) should be parsed ok") {
    val mockedNow = 1414767928000L
    val mockedParser = new InfluxQueryParser() {
      override def getConfiguredWindows: Seq[FiniteDuration] = Seq(FiniteDuration(5, TimeUnit.MINUTES))
      override def now: Long = mockedNow
    }

    val criteriaNow = mockedParser.parse("select aValue from \"metricA\" where time > now() group by time(5m)")
    val filterNow = criteriaNow.filters(0).asInstanceOf[TimeFilter]
    filterNow.identifier should be("time")
    filterNow.operator should be(Operators.Gt)
    filterNow.value should be(mockedNow)

    val criteriaNow20s = mockedParser.parse("select aValue from \"metricA\" where time < now() - 20s group by time(5m)")
    val filterNow20s = criteriaNow20s.filters(0).asInstanceOf[TimeFilter]
    filterNow20s.identifier should be("time")
    filterNow20s.operator should be(Operators.Lt)
    filterNow20s.value should be(mockedNow - TimeUnit.SECONDS.toMillis(20))

    val criteriaNow5m = mockedParser.parse("select aValue from \"metricA\" where time <= now() - 5m group by time(5m)")
    val filterNow5m = criteriaNow5m.filters(0).asInstanceOf[TimeFilter]
    filterNow5m.identifier should be("time")
    filterNow5m.operator should be(Operators.Lte)
    filterNow5m.value should be(mockedNow - TimeUnit.MINUTES.toMillis(5))

    val criteriaNow3h = mockedParser.parse("select aValue from \"metricA\" where time >= now() - 3h group by time(5m)")
    val filterNow3h = criteriaNow3h.filters(0).asInstanceOf[TimeFilter]
    filterNow3h.identifier should be("time")
    filterNow3h.operator should be(Operators.Gte)
    filterNow3h.value should be(mockedNow - TimeUnit.HOURS.toMillis(3))

    val criteriaNow10d = mockedParser.parse("select aValue from \"metricA\" where time >= now() - 10d group by time(5m)")
    val filterNow10d = criteriaNow10d.filters(0).asInstanceOf[TimeFilter]
    filterNow10d.identifier should be("time")
    filterNow10d.operator should be(Operators.Gte)
    filterNow10d.value should be(mockedNow - TimeUnit.DAYS.toMillis(10))

    val criteriaNow2w = mockedParser.parse("select aValue from \"metricA\" where time <= now() - 2w group by time(5m)")
    val filterNow2w = criteriaNow2w.filters(0).asInstanceOf[TimeFilter]
    filterNow2w.identifier should be("time")
    filterNow2w.operator should be(Operators.Lte)
    filterNow2w.value should be(mockedNow - TimeUnit.DAYS.toMillis(14))
  }

  test("Between clause should be parsed ok") {
    val query = "select aValue from \"metricA\" where time between 1414508614 and 1414509500s group by time(2h)"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
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

    influxCriteria.groupBy.duration.length should be(2)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.HOURS)

    influxCriteria.limit should be(None)
  }

  test("Group by clause by valid windows should be parsed ok") {
    val influxCriteriaResult30s = parser.parse("select aValue as counter from \"metricA\" group by time(30s)")
    influxCriteriaResult30s.groupBy.duration.length should be(30)
    influxCriteriaResult30s.groupBy.duration.unit should be(TimeUnit.SECONDS)

    val influxCriteriaResult1m = parser.parse("select aValue as counter from \"metricA\" group by time(1m)")
    influxCriteriaResult1m.groupBy.duration.length should be(1)
    influxCriteriaResult1m.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult5m = parser.parse("select aValue as counter from \"metricA\" group by time(5m)")
    influxCriteriaResult5m.groupBy.duration.length should be(5)
    influxCriteriaResult5m.groupBy.duration.unit should be(TimeUnit.MINUTES)

    val influxCriteriaResult1h = parser.parse("select aValue as counter from \"metricA\" group by time(2h)")
    influxCriteriaResult1h.groupBy.duration.length should be(2)
    influxCriteriaResult1h.groupBy.duration.unit should be(TimeUnit.HOURS)

  }

  test("Limit clause should be parsed ok") {
    val query = "select aValue from \"metricA\" group by time(1m) limit 10"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
    resultedField.name should be("aValue")
    resultedField.alias should be(None)

    influxCriteria.table.name should be("metricA")
    influxCriteria.table.alias should be(None)

    influxCriteria.groupBy.duration.length should be(1)
    influxCriteria.groupBy.duration.unit should be(TimeUnit.MINUTES)

    influxCriteria.filters should be(Nil)
    influxCriteria.limit should be(Some(10))
  }

  test("Order clause should be parsed ok") {
    val influxCriteriaAsc = parser.parse("select aValue from \"metricA\" group by time(1m) order asc")
    influxCriteriaAsc.orderAsc should be(true)

    val influxCriteriaDesc = parser.parse("select aValue from \"metricA\" group by time(1m) order desc")
    influxCriteriaDesc.orderAsc should be(false)
  }

  test("Full Influx query should be parsed ok") {
    val query = "select count(value) as counter from \"metricA\" where time > 1000 and time <= 5000 and host <> 'aHost' group by time(30s) limit 550 order desc;"
    val influxCriteria = parser.parse(query)

    val resultedField = influxCriteria.projections(0).asInstanceOf[Field]
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

    influxCriteria.orderAsc should be(false)
  }

  test("Query without projection should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select from \"metricA\"") }
  }

  test("Query without from clause should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) ") }
  }

  test("Query without table should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) from") }
  }

  test("Query with unclosed string literal should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) from \"metricA\" where host = 'host") }
  }

  test("Query with unclosed parenthesis should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) from \"metricA\" group by time(30s") }
  }

  test("Query with unconfigured time window should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) from \"metricA\" group by time(4m)") }
  }

  test("Query with invalid time now expression should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select max(value) from \"metricA\" where time  > now() - 1j group by time(30s)") }
  }

  test("Select * with other projection should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select * aValue from \"metricA\" group by time(30s)") }
  }

  test("Select with unknown order should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select * from \"metricA\" group by time(30s) order inexistentOrder") }
  }

  test("Select with invalid percentile function should fail") {
    intercept[UnsupportedOperationException] { parser.parse("select percentiles(12) from \"metricA\" group by time(30s)") }
  }

}