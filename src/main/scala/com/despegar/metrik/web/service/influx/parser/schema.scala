package com.despegar.metrik.web.service.influx.parser

import java.sql._
import java.util.Properties

case class TableSchema(name: String)

case class TableColumn(name: String, tpe: DataType) extends PrettyPrinters {
  def scalaStr: String =
    "TableColumn(" + _q(name) + ", " + tpe.toString + ")"
}

class Definitions(val defns: Map[String, Seq[TableColumn]]) extends PrettyPrinters {

  def tableExists(table: String): Boolean = defns.contains(table)

  def lookup(tableName: String, col: String): Option[TableColumn] = {
    defns.get(tableName).flatMap(_.filter(_.name == col).headOption)
  }

  def scalaStr: String = {
    "new Definitions(Map(" + (defns.map {
      case (k, v) ⇒
        (_q(k), "Seq(" + v.map(_.scalaStr).mkString(", ") + ")")
    }.map { case (k, v) ⇒ k + " -> " + v }.mkString(", ")) + "))"
  }
}
