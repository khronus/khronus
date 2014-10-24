package com.despegar.metrik.web.service.influx.parser

import scala.collection.mutable.{ ArrayBuffer, HashMap }

abstract trait Symbol {
  val ctx: Context
}

// a symbol corresponding to a reference to a column from a table in this context.
// note that this column is not necessarily projected in this context
case class ColumnSymbol(tableName: String, column: String, ctx: Context) extends Symbol

// a symbol corresponding to a reference to a projection in this context.
// note b/c of sql scoping rules, this symbol can only appear in the keys of a
// {group,order} by clause (but not the having clause of a group by)
case class ProjectionSymbol(name: String, ctx: Context) extends Symbol

abstract trait ProjectionType
case class NamedProjection(name: String, expr: Expression, pos: Int) extends ProjectionType
case object WildcardProjection extends ProjectionType

class Context(val parent: Either[Definitions, Context]) {
  var tableSchema: TableSchema = _
  val projections = new ArrayBuffer[ProjectionType]

  // is this context a parent of that context?
  def isParentOf(that: Context): Boolean = {
    var cur = that.parent.right.toOption.orNull
    while (cur ne null) {
      if (cur eq this) return true
      cur = cur.parent.right.toOption.orNull
    }
    false
  }

  // lookup a projection with the given name in this context.
  // if there are multiple projections with
  // the given name, then it is undefined which projection is returned.
  // this also returns projections which exist due to wildcard projections.
  def lookupProjection(name: String): Option[Expression] =
    lookupProjection0(name, true)

  // ignores wildcards, and treats preceeding wildcards as a single projection
  // in terms of index calculation
  def lookupNamedProjectionIndex(name: String): Option[Int] = {
    projections.zipWithIndex.flatMap {
      case (NamedProjection(n0, _, _), idx) if n0 == name ⇒ Some(idx)
      case _ ⇒ None
    }.headOption
  }

  private def lookupProjection0(name: String, allowWildcard: Boolean): Option[Expression] = {
    projections.flatMap {
      case NamedProjection(n0, expr, _) if n0 == name ⇒ Some(expr)
      case WildcardProjection if allowWildcard ⇒
        def lookupTable(tableSchema: TableSchema): Option[Expression] =
            defns.lookup(tableSchema.name, name).map(tc ⇒ {
              FieldIdent(Some(tableSchema.name), name, ColumnSymbol(tableSchema.name, name, this), this)
            })

          lookupTable(tableSchema)
      case _ ⇒ None
    }.headOption
  }

  val defns = lookupDefns()

  private def lookupDefns(): Definitions = parent match {
    case Left(d)  ⇒ d
    case Right(p) ⇒ p.lookupDefns()
  }

  // finds an column by name
  def lookupColumn(qual: Option[String], name: String, inProjScope: Boolean): Seq[Symbol] =
    lookupColumn0(qual, name, inProjScope, None)

  private def lookupColumn0(qual: Option[String],
    name: String,
    inProjScope: Boolean,
    topL: Option[String]): Seq[Symbol] = {

    def lookupSymbolsByTable(topLevel: String, tableSchema: TableSchema): Seq[Symbol] =
        defns.lookup(tableSchema.name, name).map(tc ⇒ ColumnSymbol(topLevel, name, this)).toSeq

    val res = qual match {
      case Some(q) ⇒
        // we give preference to column lookups first
        lookupSymbolsByTable(q, tableSchema)
      case None ⇒
        // projection lookups
        val r = lookupSymbolsByTable(tableSchema.name, tableSchema)
        if (r.isEmpty) {
          lookupProjection0(name, false).map(_ ⇒ Seq(ProjectionSymbol(name, this))).getOrElse(Seq.empty)
        } else r
    }

    if (res.isEmpty) {
      // TODO:
      // lookup in parent- assume loo
      // kups in parent scope never reads projections-
      // ie we currently assume no sub-selects in {group,order} by clauses
      parent.right.toOption.map(_.lookupColumn(qual, name, false)).getOrElse(Seq.empty)
    } else res
  }

  // HACK: don't use default toString(), so that built-in ast node's
  // toString can be used for structural equality
  override def toString = "Context"
}
