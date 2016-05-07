package com.searchlight.khronus.query

import com.searchlight.khronus.api.Series

import scala.concurrent.Future

trait DynamicSQLQueryServiceSupport {
  def dynamicSQLQueryService: DynamicSQLQueryService = DynamicSQLQueryService.instance
}

trait DynamicSQLQueryService {
  def executeSQLQuery(query: String): Future[Seq[Series]]
}

object DynamicSQLQueryService {
  val instance = new DefaultDynamicSQLQueryService
}

class DefaultDynamicSQLQueryService extends DynamicSQLQueryService with SQLParserSupport with DynamicQueryExecutorSupport {

  def executeSQLQuery(query: String): Future[Seq[Series]] = {
    dynamicQueryExecutor.execute(parser.parse(query))
  }

}
