package com.searchlight.khronus.query.projection

/*
case class Div(left: ProjectionObject, right: ProjectionObject) extends ProjectionObject {
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
*/ 