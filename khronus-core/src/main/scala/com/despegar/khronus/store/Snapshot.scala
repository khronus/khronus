package com.despegar.khronus.store

import java.util.concurrent.TimeUnit

import com.despegar.khronus.util.ConcurrencySupport
import com.despegar.khronus.util.log.Logging

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

trait Snapshot[T] extends Logging with ConcurrencySupport {

  implicit def context: ExecutionContext

  def snapshotName: String

  @volatile
  var snapshot: T = _

  private val pool = scheduledThreadPool(s"$snapshotName-snapshot-reload-worker")
  pool.scheduleAtFixedRate(reload(), 0, 5, TimeUnit.SECONDS)

  private def reload() = new Runnable {
    override def run(): Unit = {
      try {
        getFreshData() onComplete {
          case Success(data) ⇒ snapshot = data
          case Failure(t)    ⇒ log.error("Error reloading data", t)
        }
      } catch {
        case reason: Throwable ⇒ log.error("Unexpected error reloading data", reason)
      }
    }
  }

  def getFromSnapshot: T = snapshot

  def getFreshData(): Future[T]

}

