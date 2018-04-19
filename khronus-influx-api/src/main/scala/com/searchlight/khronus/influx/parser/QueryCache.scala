package com.searchlight.khronus.influx.parser

import java.util.concurrent.TimeUnit

import com.searchlight.khronus.util.log.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait QueryCache extends Logging {
  val regexWitNames = new util.matching.Regex(".*(now.{2}-([0-9]+)([a-z]+)|> ([0-9]+)(s) and time < ([0-9]+)(s)) group.*", "all", "relativeTime", "relativeSuffix", "secondsGreater", "greaterSuffix", "secondsMinor", "minorSuffix")

  private def toMillis(number: String, suffix: String): Long = suffix match {
    case TimeSuffixes.Seconds ⇒ TimeUnit.SECONDS.toMillis(number.toLong)
    case TimeSuffixes.Minutes ⇒ TimeUnit.MINUTES.toMillis(number.toLong)
    case TimeSuffixes.Hours   ⇒ TimeUnit.HOURS.toMillis(number.toLong)
    case TimeSuffixes.Days    ⇒ TimeUnit.DAYS.toMillis(number.toLong)
    case TimeSuffixes.Weeks   ⇒ TimeUnit.DAYS.toMillis(number.toLong) * 7L
    case _                    ⇒ number.toLong
  }

  def delta(now: Long, relativeTime: String, relativeSuffix: String): Long = {
    now - toMillis(relativeTime, relativeSuffix)
  }

  def putInCache(query: String, parsedQuery: InfluxCriteria, now: Long)(implicit cache: scala.collection.concurrent.Map[String, (Boolean, InfluxCriteria)]): Boolean = {
    //    val timeFilters = parsedQuery.filters.filter(_.isInstanceOf[TimeFilter])
    //    if (timeFilters.size != 1) {
    //      log.info(s"Query has more than one time filter and not apply for cache: $query")
    //      return false
    //    }

    val cacheCandidate: InfluxCriteria = refreshTimeFilter(query, parsedQuery, now)
    if (cacheCandidate.equals(parsedQuery)) {
      cache.put(query, (true, parsedQuery))
      return true
    } else {
      cache.put(query, (false, null))
      log.info(s"Query not apply for cache for unknown reasons: $query")
      return false
    }
  }

  private def refreshTimeFilter(query: String, parsedQuery: InfluxCriteria, now: Long): InfluxCriteria = {
    val timeFilters = parsedQuery.filters.filter(_.isInstanceOf[TimeFilter])

    val groups = regexWitNames.findFirstMatchIn(query).get

    if (timeFilters.lengthCompare(1) == 0) {
      val timeFilter = timeFilters.last.asInstanceOf[TimeFilter]
      return parsedQuery.copy(filters = Seq(timeFilter.copy(value = delta(now, groups.group("relativeTime"), groups.group("relativeSuffix")))))
    } else if (timeFilters.lengthCompare(2) == 0) {
      val timeFilterGreater = timeFilters(0).asInstanceOf[TimeFilter]
      val timeFilterMinor = timeFilters(1).asInstanceOf[TimeFilter]
      return parsedQuery.copy(filters = Seq(timeFilterGreater.copy(value = toMillis(groups.group("secondsGreater"), groups.group("greaterSuffix"))),
        timeFilterMinor.copy(value = toMillis(groups.group("secondsMinor"), groups.group("minorSuffix")))))
    }

    throw new UnsupportedOperationException(s"not support query cache for $query")
  }

  def cacheQuery(query: String)(block: Long ⇒ Future[InfluxCriteria])(implicit cache: scala.collection.concurrent.Map[String, (Boolean, InfluxCriteria)]): Future[InfluxCriteria] = {
    val now = System.currentTimeMillis()
    cache.get(query) match {
      case None ⇒ {
        block(now).map(parsedQuery ⇒ {
          putInCache(query, parsedQuery, now)
          parsedQuery
        })
      }

      case Some((isValid, cacheQuery)) ⇒ if (isValid) Future.successful(refreshTimeFilter(query, cacheQuery, now)) else block(now)
    }
  }
}
