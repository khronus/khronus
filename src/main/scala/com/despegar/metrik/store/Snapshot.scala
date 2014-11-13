package com.despegar.metrik.store

import java.util.concurrent.{ TimeUnit, Executors }

import com.despegar.metrik.model.Metric
import com.despegar.metrik.util.Logging

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

trait Snapshot[T] extends Logging {

  implicit def context: ExecutionContext

  var snapshot: T = _

  private val pool = Executors.newScheduledThreadPool(1)

  pool.scheduleAtFixedRate(reload(), 1, 5, TimeUnit.SECONDS)

  private def reload() = new Runnable {
    override def run(): Unit = {
      getFreshData() onComplete {
        case Success(data) ⇒ snapshot = data
        case Failure(t)    ⇒ log.error("Error reloading data", t)
      }
    }
  }

  def getFromSnapshot: T = snapshot

  def getFreshData(): Future[T]

}

