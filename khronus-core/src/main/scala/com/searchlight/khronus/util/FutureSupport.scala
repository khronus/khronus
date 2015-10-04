package com.searchlight.khronus.util

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }

trait FutureSupport {
  import scala.language.higherKinds

  // thanks http://www.michaelpollmeier.com/execute-scala-futures-in-serial-one-after-the-other-non-blocking/
  def sequenced[A, B, C[A] <: Iterable[A]](collection: C[A])(fn: A ⇒ Future[B])(
    implicit ec: ExecutionContext, cbf: CanBuildFrom[C[B], B, C[B]]): Future[C[B]] = {
    val builder = cbf()
    builder.sizeHint(collection.size)

    collection.foldLeft(Future(builder)) {
      (previousFuture, next) ⇒
        for {
          previousResults ← previousFuture
          next ← fn(next)
        } yield previousResults += next
    } map { builder ⇒ builder.result }
  }
}
