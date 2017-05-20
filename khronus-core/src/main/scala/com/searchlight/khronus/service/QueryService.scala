package com.searchlight.khronus.service

import com.searchlight.khronus.dao.{ Meta, MetaStore }
import com.searchlight.khronus.model.query.{ Query, TimeRange, TimeRangeMillis }
import com.searchlight.khronus.model.{ Functions, Series, Summary }
import com.searchlight.khronus.util.Settings
import com.searchlight.khronus.util.log.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, _ }

case class QueryService(bucketQueryService: BucketQueryService = BucketQueryService(),
    summaryService: SummaryService = SummaryService(),
    metaStore: MetaStore = Meta.metaStore) extends Logging {

  private val maxResolution = Settings.Dashboard.MaxResolutionPoints
  private val minResolution = Settings.Dashboard.MinResolutionPoints
  private val forceResolution = true

  def executeQuery(query: Query): Future[Seq[Series]] = {
    executeAsBucketQuery(withProperDefaults(query))
    /*if (canUseSummaries(query)) {
      executeAsSummaryQuery(query)
    } else {
    }*/
  }

  private def withProperDefaults(query: Query): Query = {
    val queryWithTimeRange = query.copy(timeRange = query.timeRange.orElse(Some(TimeRange(to = System.currentTimeMillis()))))
    val queryWithResolution = queryWithTimeRange.copy(resolution = Some(resolveResolution(queryWithTimeRange)))
    queryWithResolution
  }

  private def resolveResolution(query: Query): Duration = {
    query.resolution.getOrElse {
      query.timeRange.get.duration
    }
  }

  private def executeAsBucketQuery(query: Query): Future[Seq[Series]] = bucketQueryService.execute(query)

  /*
private def getSummariesBySourceMap(query: Query, timeWindow: Duration, slice: TimeRange) = {
  query.metrics.foldLeft(Map.empty[String, Future[Map[Long, Summary]]])((acc, qmetric) ⇒ {
    val metric = metaStore.getMetricByName(qmetric.name)
    val tableId = qmetric.alias
    val summariesByTs = summaryService.readSummaries(metric, query).map(f ⇒ f.foldLeft(Map.empty[Long, Summary])((acc, summary) ⇒ acc + (summary.timestamp.ms -> summary)))
    acc + (tableId -> summariesByTs)
  })
}
*/

  private def buildSeries(query: Query, timeRangeMillis: TimeRangeMillis,
    summariesBySourceMap: Map[String, Future[Map[Long, Summary]]]): Seq[Future[Series]] = {

    // query.projections.sortBy(_.alias)
    /*
    influxCriteria.projections.sortBy(_.seriesId).map {
      case field: Field ⇒ {
        generateSeq(field, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
          toInfluxSeries(values, field.alias.getOrElse(field.name), influxCriteria.orderAsc, influxCriteria.scale, field.tableId.get))
      }
      case number: Number ⇒ {
        generateSeq(number, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
          toInfluxSeries(values, number.alias.get, influxCriteria.orderAsc, influxCriteria.scale))
      }
      case operation: Operation ⇒ {
        for {
          leftValues ← generateSeq(operation.left, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
          rightValues ← generateSeq(operation.right, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
        } yield {
          val resultedValues = zipByTimestamp(leftValues, rightValues, operation.operator)
          toInfluxSeries(resultedValues, operation.alias, influxCriteria.orderAsc, influxCriteria.scale)
        }
      }
    }*/
    ???
  }

  private def generateScalarSeq(timeRangeMillis: TimeRangeMillis, scalar: Double): Future[Map[Long, Double]] = {
    Future {
      (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).map(ts ⇒ ts -> scalar).toMap
    }
  }

  private def generateSummarySeq(timeRangeMillis: TimeRangeMillis, function: Functions.Function, summariesByTs: Future[Map[Long, Summary]], defaultValue: Option[Double]): Future[Map[Long, Double]] = {
    summariesByTs.map(summariesMap ⇒ {
      (timeRangeMillis.from to timeRangeMillis.to by timeRangeMillis.timeWindow).foldLeft(Map.empty[Long, Double])((acc, currentTimestamp) ⇒
        if (summariesMap.get(currentTimestamp).isDefined) {
          function match {
            case metaFunction: Functions.MetaFunction ⇒ acc + (currentTimestamp -> metaFunction(summariesMap(currentTimestamp), timeRangeMillis.timeWindow))
            case simpleFunction: Functions.Function   ⇒ acc + (currentTimestamp -> simpleFunction(summariesMap(currentTimestamp)))
          }
        } else if (defaultValue.isDefined) {
          acc + (currentTimestamp -> defaultValue.get)
        } else {
          acc
        })
    })
  }

  /*
    private def zipByTimestamp(tsValues1: Map[Long, Double], tsValues2: Map[Long, Double], operator: MathOperators.MathOperator): Map[Long, Double] = {
      val zippedByTimestamp = for (timestamp ← tsValues1.keySet.intersect(tsValues2.keySet))
        yield (timestamp, calculate(tsValues1(timestamp), tsValues2(timestamp), operator))

      zippedByTimestamp.toMap
    }

    private def calculate(firstOperand: Double, secondOperand: Double, operator: MathOperators.MathOperator): Double = {
      operator(firstOperand, secondOperand)
    }

    private def toInfluxSeries(timeSeriesValues: Map[Long, Double], projectionName: String, ascendingOrder: Boolean, scale: Option[Double], metricName: String = ""): InfluxSeries = {
      log.debug(s"Building Influx series for projection [$projectionName] - Metric [$metricName]")

      val sortedTimeSeriesValues = if (ascendingOrder) timeSeriesValues.toSeq.sortBy(_._1) else timeSeriesValues.toSeq.sortBy(-_._1)

      val points = sortedTimeSeriesValues.foldLeft(Vector.empty[Vector[AnyVal]])((acc, current) ⇒ {
        val value = BigDecimal(current._2 * scale.getOrElse(1d)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        acc :+ Vector(current._1, value)
      })
      InfluxSeries(metricName, Vector(influxTimeKey, projectionName), points)
    }

    private def generateSeq(simpleProjection: SimpleProjection, timeRangeMillis: TimeRangeMillis, summariesMap: Map[String, Future[Map[Long, Summary]]], defaultValue: Option[Double]): Future[Map[Long, Double]] =
      simpleProjection match {
        case field: Field ⇒ generateSummarySeq(timeRangeMillis, Functions.withName(field.name), summariesMap(field.tableId.get), defaultValue)
        case number: Number ⇒ generateScalarSeq(timeRangeMillis, number.value)
        case _ ⇒ throw new UnsupportedOperationException("Nested operations are not supported yet")
      }

    private def buildInfluxSeries(influxCriteria: InfluxCriteria, timeRangeMillis: TimeRangeMillis, summariesBySourceMap: Map[String, Future[Map[Long, Summary]]]): Seq[Future[InfluxSeries]] = {
      influxCriteria.projections.sortBy(_.seriesId).map {
        case field: Field ⇒ {
          generateSeq(field, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
            toInfluxSeries(values, field.alias.getOrElse(field.name), influxCriteria.orderAsc, influxCriteria.scale, field.tableId.get))
        }
        case number: Number ⇒ {
          generateSeq(number, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue).map(values ⇒
            toInfluxSeries(values, number.alias.get, influxCriteria.orderAsc, influxCriteria.scale))
        }
        case operation: Operation ⇒ {
          for {
            leftValues ← generateSeq(operation.left, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
            rightValues ← generateSeq(operation.right, timeRangeMillis, summariesBySourceMap, influxCriteria.fillValue)
          } yield {
            val resultedValues = zipByTimestamp(leftValues, rightValues, operation.operator)
            toInfluxSeries(resultedValues, operation.alias, influxCriteria.orderAsc, influxCriteria.scale)
          }
        }
      }
    }

  private def executeAsSummaryQuery(query: Query): Future[Seq[Series]] = {
    val resolution = query.resolution.get
    val slice: TimeRange = query.timeRange
    val timeRangeMillis = buildTimeRangeMillis(slice, resolution)
    val summariesBySourceMap = getSummariesBySourceMap(query, resolution, slice)
    val series: Seq[Future[Series]] = buildSeries(query, timeRangeMillis, summariesBySourceMap)
    Future.sequence(series)
  }
  private def canUseSummaries(query: Query): Boolean = {
    !metaStore.hasDimensions(metaStore.searchMetrics(query.metrics.head.name).head)
  }
  */
  private def buildTimeRangeMillis(slice: TimeRange, timeWindow: Duration): TimeRangeMillis = {
    val alignedFrom = alignTimestamp(slice.from, timeWindow, floorRounding = false)
    val alignedTo = alignTimestamp(slice.to, timeWindow, floorRounding = true)
    TimeRangeMillis(alignedFrom, alignedTo, timeWindow.toMillis)
  }

  private def alignTimestamp(timestamp: Long, timeWindow: Duration, floorRounding: Boolean): Long = {
    if (timestamp % timeWindow.toMillis == 0)
      timestamp
    else {
      val division = timestamp / timeWindow.toMillis
      if (floorRounding) division * timeWindow.toMillis else (division + 1) * timeWindow.toMillis
    }
  }

}

