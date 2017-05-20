package com.searchlight.khronus.service

import com.searchlight.khronus.model.query._
import com.searchlight.khronus.model.{ Point, Series }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class ProjectionService() {

  private def calculate(a: Future[Series], b: Future[Series], operation: (Double, Double) ⇒ Double, operatorLabel: String): Future[Series] = {
    //Series(s"${a.name} $operatorLabel ${b.name}", ???)
    ???
  }

  private def calculateProjection(projection: Projection, groups: Seq[Group]): Seq[Future[Series]] = {
    projection match {
      case function: Function ⇒ {
        groups.filter(_.selector == function.selector).map { group ⇒
          calculatePoints(projection, group) map { points ⇒
            Series(s"${group.label}_${function.label}", points)
          }
        }
      }
      case operator: Operator ⇒ {
        groups.groupBy(group ⇒ group.dimensions).flatMap {
          case (dimensions, groupsByDimensions) ⇒
            val seriesSeqA = calculateProjection(operator.a, groupsByDimensions)
            val seriesSeqB = calculateProjection(operator.b, groupsByDimensions)
            seriesSeqA.flatMap { seriesA ⇒
              seriesSeqB.map { seriesB ⇒
                calculate(seriesA, seriesB, operator.calculation, operator.label)
              }
            }
        }.toSeq
      }
      case _ ⇒ Seq()
    }
  }

  private def calculatePoints(projection: Projection, group: Group): Future[Seq[Point]] = {
    group.buckets.map { buckets ⇒
      buckets.map {
        case (bucketNumber, lazyBucket) ⇒
          Point(bucketNumber.startTimestamp().ms, projection.value(lazyBucket()))
      }.toSeq
    }
  }

  def calculate(query: Query, groups: Seq[Group]): Future[Seq[Series]] = {
    Future.sequence(query.projections.flatMap { projection ⇒
      calculateProjection(projection, groups)
    })
  }

}

