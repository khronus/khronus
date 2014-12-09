package com.despegar.metrik.store

import java.util.concurrent.{ Executors, TimeUnit }

import com.despegar.metrik.util.ConcurrencySupport

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }
import com.despegar.metrik.util.log.Logging

trait Snapshot[T] extends Logging with ConcurrencySupport {

  implicit def context: ExecutionContext
  val snapshotName: String

  var snapshot: T = _

  private val pool = scheduledThreadPool(s"$snapshotName-snapshot-reload-worker", 1)
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

