package com.searchlight.khronus.service

import com.searchlight.khronus.api.sql.SQLParser
import com.searchlight.khronus.model.Series

import scala.concurrent.Future

case class SQLQueryService(parser: SQLParser, queryService: QueryService) {

  def execute(sqlQuery: String): Future[Seq[Series]] = {
    val query = parser.parse(sqlQuery)
    queryService.executeQuery(query)
  }

}
