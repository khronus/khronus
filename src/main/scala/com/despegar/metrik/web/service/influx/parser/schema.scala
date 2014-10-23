package com.despegar.metrik.web.service.influx.parser

import java.sql._
import java.util.Properties

// these are the types of relations which can show up in a
// FROM clause
abstract trait Relation
case class TableRelation(name: String) extends Relation

case class TableColumn(name: String, tpe: DataType) extends PrettyPrinters {
  def scalaStr: String =
    "TableColumn(" + _q(name) + ", " + tpe.toString + ")"
}

class Definitions(val defns: Map[String, Seq[TableColumn]]) extends PrettyPrinters {

  def tableExists(table: String): Boolean = defns.contains(table)

  def lookup(table: String, col: String): Option[TableColumn] = {
    defns.get(table).flatMap(_.filter(_.name == col).headOption)
  }

  def scalaStr: String = {
    "new Definitions(Map(" + (defns.map {
      case (k, v) ⇒
        (_q(k), "Seq(" + v.map(_.scalaStr).mkString(", ") + ")")
    }.map { case (k, v) ⇒ k + " -> " + v }.mkString(", ")) + "))"
  }
}

trait Schema {
  def loadSchema(): Definitions
}

class PgSchema(hostname: String, port: Int, db: String, props: Properties) extends Schema {
  Class.forName("org.postgresql.Driver")
  private val conn = DriverManager.getConnection(
    "jdbc:postgresql://%s:%d/%s".format(hostname, port, db), props)

  def loadSchema() = {
    import Conversions._
    val s = conn.prepareStatement("""
select table_name from information_schema.tables
where table_catalog = ? and table_schema = 'public'
      """)
    s.setString(1, db)
    val r = s.executeQuery
    val tables = r.map(_.getString(1))
    s.close()

    new Definitions(tables.map(name ⇒ {
      val s = conn.prepareStatement("""
select
  column_name, data_type, character_maximum_length,
  numeric_precision, numeric_precision_radix, numeric_scale
from information_schema.columns
where table_schema = 'public' and table_name = ?
""")
      s.setString(1, name)
      val r = s.executeQuery
      val columns = r.map(rs ⇒ {
        val cname = rs.getString(1)
        TableColumn(cname, rs.getString(2) match {
          case "character varying" ⇒ VariableLenString(rs.getInt(3))
          case "character"         ⇒ FixedLenString(rs.getInt(3))
          case "date"              ⇒ DateType
          case "numeric"           ⇒ DecimalType(rs.getInt(4), rs.getInt(6))
          case "integer" ⇒
            assert(rs.getInt(4) % 8 == 0)
            IntType(rs.getInt(4) / 8)
          case "bigint"   ⇒ IntType(8)
          case "smallint" ⇒ IntType(2)
          case "bytea"    ⇒ VariableLenByteArray(None)
          case e          ⇒ sys.error("unknown type: " + e)
        })
      })
      s.close()
      (name, columns)
    }).toMap)
  }
}
