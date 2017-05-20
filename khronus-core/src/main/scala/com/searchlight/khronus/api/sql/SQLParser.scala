package com.searchlight.khronus.api.sql

import com.searchlight.khronus.api.sql.jsql.JSQLParser
import com.searchlight.khronus.model.query.{ Query, Selector }

trait SQLParser {
  def parse(query: String): Query
}

object SQLParser {
  val instance = new JSQLParser

  def selector(id: String, selectors: Seq[Selector]): Selector = {
    selectors.find(selector ⇒ selector.regex.equals(id) || selector.alias.exists(alias ⇒ alias.equals(id))).getOrElse {
      throw new RuntimeException(s"Unknown selector $id")
    }
  }
}

