package com.searchlight.khronus.influx.parser

import java.util.concurrent.TimeUnit

import com.searchlight.khronus.util.log.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait QueryCache extends Logging {
  val regexTime = ".*now.{2}-([0-9]+)([a-z]+) group.*".r

  private def toMillis(number: String, suffix: String): Long = suffix match {
    case TimeSuffixes.Seconds ⇒ TimeUnit.SECONDS.toMillis(number.toLong)
    case TimeSuffixes.Minutes ⇒ TimeUnit.MINUTES.toMillis(number.toLong)
    case TimeSuffixes.Hours   ⇒ TimeUnit.HOURS.toMillis(number.toLong)
    case TimeSuffixes.Days    ⇒ TimeUnit.DAYS.toMillis(number.toLong)
    case TimeSuffixes.Weeks   ⇒ TimeUnit.DAYS.toMillis(number.toLong) * 7L
    case _                    ⇒ number.toLong
  }

  def delta(now: Long, query: String): Long = {
    now - (query match {
      case regexTime(time, preffix) ⇒ toMillis(time, preffix)
      case _                        ⇒ 0
    })
  }

  def putInCache(query: String, parsedQuery: InfluxCriteria, now: Long)(implicit cache: scala.collection.concurrent.Map[String, (Boolean, InfluxCriteria)]): Boolean = {
    val timeFilters = parsedQuery.filters.filter(_.isInstanceOf[TimeFilter])
    if (timeFilters.size != 1) {
      log.info(s"Query has more than one time filter and not apply for cache: $query")
      return false
    }

    val timeFilter = timeFilters.last.asInstanceOf[TimeFilter]
    val cacheCandidate = parsedQuery.copy(filters = Seq(timeFilter.copy(value = delta(now, query))))
    if (cacheCandidate.equals(parsedQuery)) {
      cache.put(query, (true, parsedQuery))
      return true
    } else {
      cache.put(query, (false, null))
      log.info(s"Query not apply for cache for unknown reasons: $query")
      return false
    }
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

      case Some((isValid, parsedQuery)) ⇒ if (isValid) Future.successful(parsedQuery) else block(now)
    }
  }
}
