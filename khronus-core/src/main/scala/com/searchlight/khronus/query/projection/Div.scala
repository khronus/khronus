package com.searchlight.khronus.query.projection

import com.searchlight.khronus.api.{ Point, Series }
import com.searchlight.khronus.model.{ Bucket, BucketSlice, Metric }
import com.searchlight.khronus.query.{ Projection, QMetric }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Div(left: Projection, right: Projection) extends Projection {
  override def execute(input: Map[QMetric, Map[Metric, Future[BucketSlice[Bucket]]]]): Option[Future[Seq[Series]]] = {
    for (
      leftFuture ← left.execute(input);
      rightFuture ← right.execute(input)
    ) yield for (leftSeries ← leftFuture; rightSeries ← rightFuture) yield div(leftSeries.head, rightSeries.head)
  }

  private def div(leftPoints: Seq[Point], rightPoints: Seq[Point]): Seq[Point] = {
    val leftValues = leftPoints.map(point ⇒ (point.timestamp, point.value)).toMap
    val rightValues = rightPoints.map(point ⇒ (point.timestamp, point.value)).toMap
    val divisions = leftPoints.flatMap(point ⇒ rightValues.get(point.timestamp).map(value ⇒ Point(point.timestamp, point.value / value)))
    val zeroes = rightPoints.filterNot(point ⇒ leftValues.get(point.timestamp).isDefined).map(point ⇒ Point(point.timestamp, 0d))
    (divisions ++ zeroes).sortBy(_.timestamp)
  }

  private def div(left: Series, right: Series): Seq[Series] = {
    Seq(Series(s"${left.name} / ${right.name}", div(left.points, right.points)))
  }
}
