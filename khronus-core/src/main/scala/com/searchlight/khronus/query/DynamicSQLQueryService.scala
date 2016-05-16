package com.searchlight.khronus.query

import com.searchlight.khronus.api.Series

import scala.concurrent.Future

trait DynamicSQLQueryServiceSupport {
  def dynamicSQLQueryService: DynamicSQLQueryService = DynamicSQLQueryService.instance
}

trait DynamicSQLQueryService {
  def executeSQLQuery(query: String): Future[Seq[Series]]
  def executeQuery(dynamicQuery: DynamicQuery): Future[Seq[Series]]
}

object DynamicSQLQueryService {
  val instance = new DefaultDynamicSQLQueryService
}

class DefaultDynamicSQLQueryService extends DynamicSQLQueryService with SQLParserSupport with DynamicQueryExecutorSupport {

  def executeSQLQuery(query: String): Future[Seq[Series]] = {
    val dynamicQuery: DynamicQuery = parser.parse(query)
    executeQuery(dynamicQuery)
  }

  def executeQuery(dynamicQuery: DynamicQuery): Future[Seq[Series]] = {
    dynamicQueryExecutor.execute(dynamicQuery)
  }
}
