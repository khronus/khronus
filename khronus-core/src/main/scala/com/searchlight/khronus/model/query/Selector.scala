package com.searchlight.khronus.model.query

case class Selector(regex: String, alias: Option[String] = None) {
  val label: String = {
    s"${alias.getOrElse(regex)}"
    //${filter.map(f => s"[${f.label}]").getOrElse("")}
  }
}
