package com.despegar.metrik.web.service

import org.scalatest.{ShouldMatchers, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.despegar.metrik.web.service.influx.parser.{MetricCriteria, InfluxQueryParser}
import org.specs2.Specification
import org.scalatest.Matchers._


/**
 * Created by aholman on 23/10/14.
 */
class InfluxQueryParserSpec extends FunSuite with ShouldMatchers {

  test("basic Influx query should be parsed ok") {
    // TODO Fechas: '2013-08-12 23:32:01.232' - now() - 1h
    // TODO group by time(30s)

    val query = "select aValue, max(value) as maxValue, min(value) as minValue from metricA where time > 1000 and time < 5000 and host = 'aHost' group by minValue limit 550;"
    val parser = new InfluxQueryParser()
    val metricCriteriaResult = parser.parse(query)

    val metricCriteria = metricCriteriaResult.get

    println(metricCriteria)
//      metricCriteria.projections should be(Some(MetricCriteria))


  }



}
