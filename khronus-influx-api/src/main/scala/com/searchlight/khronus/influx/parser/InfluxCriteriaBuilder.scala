package com.searchlight.khronus.influx.parser

import scala.concurrent.{ ExecutionContext, Future }
import com.searchlight.khronus.model.{ Functions, MetricType }
import com.searchlight.khronus.store.MetaSupport
import com.searchlight.khronus.util.ConcurrencySupport

trait InfluxCriteriaBuilder extends MetaSupport with ConcurrencySupport {

  implicit val ex: ExecutionContext = executionContext("influx-query-parser-worker")

  def buildInfluxCriteria(tables: Seq[Table], projections: Seq[Projection], filters: Seq[Filter], groupBy: GroupBy, fill: Option[Double], scale: Option[Double], order: Boolean, limit: Int): Future[InfluxCriteria] = {
    validateAlias(projections, tables)

    val futureSources = tables.collect { case table ⇒ getSources(table) }

    Future.sequence(futureSources).map(f ⇒ {
      val sources = f.flatten
      val simpleProjections = buildProjections(projections, sources)
      InfluxCriteria(simpleProjections, sources, filters, groupBy, fill, scale, limit, order)
    })
  }

  private def validateAlias(projections: Seq[Projection], tables: Seq[Table]): Unit = {
    val aliasTables = tables.filter(_.alias.isDefined).map(_.alias.get)
    if (aliasTables.toSet.size < aliasTables.size) throw new UnsupportedOperationException("Different metrics can't use the same alias")

    validateProjectionAlias(projections, aliasTables)
  }

  private def validateProjectionAlias(projections: Seq[Projection], aliasTables: Seq[String]): Unit = {
    projections.foreach {
      case field: AliasingTable ⇒
        if (field.tableId != None && !aliasTables.contains(field.tableId.get))
          throw new UnsupportedOperationException(s"Projection is using an invalid alias: ${field.tableId.get} - Table alias: [${aliasTables.mkString(", ")}]")
      case number: Number ⇒ // Numbers dont have alias
      case operation: Operation ⇒
        validateProjectionAlias(Seq(operation.left), aliasTables)
        validateProjectionAlias(Seq(operation.right), aliasTables)
    }
  }

  private def getSources(table: Table): Future[Seq[Source]] = {
    val matchedMetrics = metaStore.searchInSnapshotByRegex(getCaseInsensitiveRegex(table.name))
    if (matchedMetrics.isEmpty)
      throw new UnsupportedOperationException(s"Unsupported query - There isnt any metric matching the regex [${table.name}]")
    else if (matchedMetrics.size > 1 && table.alias != None)
      throw new UnsupportedOperationException(s"Unsupported query - Regex [${table.name}] matches more than one metric, so it can't have an alias (${table.alias}})")

    Future.successful(matchedMetrics.collect { case m ⇒ Source(m, table.alias) })
  }

  def getCaseInsensitiveRegex(metricNameRegex: String) = {
    val caseInsensitiveRegex = "(?i)"
    s"$caseInsensitiveRegex$metricNameRegex"
  }

  private def buildProjections(projections: Seq[Projection], sources: Seq[Source]): Seq[SimpleProjection] = {
    projections.collect {
      case AllField(tableAlias) ⇒ buildAllFields(tableAlias, sources)
      case function: Field      ⇒ buildField(function, sources)
      case n: Number            ⇒ Seq(n)
      case op: Operation ⇒
        val left = buildProjections(Seq(op.left), sources).head
        val right = buildProjections(Seq(op.right), sources).head
        Seq(Operation(left, right, op.operator, op.alias))
    }.flatten

  }

  private def buildField(function: Field, sources: Seq[Source]): Seq[Field] = {
    val matchedSources = function.tableId match {
      case Some(alias) ⇒ Seq(lookupByAlias(alias, sources))
      case None        ⇒ sources
    }

    matchedSources.map {
      source ⇒
        validateByMetricType(source.metric.mtype, function)
        Seq(Field(function.name, function.alias, Some(source.alias.getOrElse(source.metric.name))))
    }.flatten
  }

  private def validateByMetricType(metricType: String, function: SimpleProjection): Unit = {
    val functionsByMetricType = allFunctionsByMetricType(metricType)
    function match {
      case field: Field ⇒
        if (!functionsByMetricType.contains(field.name))
          throw new UnsupportedOperationException(s"${field.name} is an invalid function for a $metricType. Valid options: [${functionsByMetricType.mkString(",")}]")
      case _ ⇒
    }
  }

  private def buildAllFields(tableAlias: Option[String], sources: Seq[Source]): Seq[Field] = {
    val matchedSources = tableAlias match {
      case Some(alias) ⇒ Seq(lookupByAlias(alias, sources))
      case None        ⇒ sources
    }

    matchedSources.map {
      source ⇒
        val functionsByMetricType = allFunctionsByMetricType(source.metric.mtype)
        functionsByMetricType.collect {
          case functionName ⇒ Field(functionName, None, Some(source.alias.getOrElse(source.metric.name)))
        }
    }.flatten
  }

  private def lookupByAlias(alias: String, sources: Seq[Source]): Source = {
    sources.filter(s ⇒ s.alias.isDefined && s.alias.get.equals(alias)).head
  }

  private def allFunctionsByMetricType(metricType: String): Seq[String] = metricType match {
    case MetricType.Timer | MetricType.Gauge ⇒ Functions.allHistogramFunctions
    case MetricType.Counter                  ⇒ Functions.allCounterFunctions
    case _                                   ⇒ throw new UnsupportedOperationException(s"Unknown metric type: $metricType")
  }

}
