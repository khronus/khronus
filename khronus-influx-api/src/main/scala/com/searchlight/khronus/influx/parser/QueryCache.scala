package com.searchlight.khronus.influx.parser

import com.searchlight.khronus.util.log.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait QueryCache extends Logging {

  def putInCache(query: String, parsedQuery: InfluxCriteria, now: Long)(implicit cache: scala.collection.concurrent.Map[String, (Boolean, InfluxCriteria)]): Boolean = {
    val timeFilters = parsedQuery.filters.filter(_.isInstanceOf[TimeFilter])
    if (timeFilters.size != 1) {
      log.info(s"Query has more than one time filter and not apply for cache: $query")
      return false
    }

    val timeFilter = timeFilters.last.asInstanceOf[TimeFilter]
    val cacheCandidate = parsedQuery.copy(filters = Seq(timeFilter.copy(value = now)))
    if (cacheCandidate.equals(parsedQuery)) {
      cache.put(query, (true, parsedQuery))
      true
    } else {
      cache.put(query, (false, null))
      log.info(s"Query not apply for cache for unknown reasons: $query")
      false
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
