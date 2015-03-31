package com.despegar.khronus.store

import java.util.concurrent.TimeUnit

import com.despegar.khronus.util.ConcurrencySupport
import com.despegar.khronus.util.log.Logging

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

trait Snapshot[T] extends Logging with ConcurrencySupport {

  implicit def context: ExecutionContext

  def snapshotName: String

  def initialValue: T

  @volatile
  var snapshot: T = initialValue

  private val pool = scheduledThreadPool(s"snapshot-reload-worker")

  def startSnapshotReloads() = {
    Try {
      snapshot = Await.result(getFreshData(), 5 seconds)
    }
    pool.scheduleAtFixedRate(reload(), 5, 5, TimeUnit.SECONDS)
  }

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

