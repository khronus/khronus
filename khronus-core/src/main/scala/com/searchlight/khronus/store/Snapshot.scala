package com.searchlight.khronus.store

import java.util.concurrent.TimeUnit

import com.searchlight.khronus.util.{ SameThreadExecutionContext, ConcurrencySupport }
import com.searchlight.khronus.util.log.Logging

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

trait Snapshot[T] extends Logging with ConcurrencySupport {

  def snapshotName: String

  def initialValue: T

  @volatile
  var snapshot: T = initialValue

  private val scheduledPool = scheduledThreadPool(s"snapshot-reload-scheduled-worker")

  //use the scheduledPool only to start the main thread. the remain operations like getFreshData() will run on this one
  implicit val context: ExecutionContext = executionContext("snapshot-reload-worker", 1)

  def startSnapshotReloads() = {
    reload()
    scheduleReloads()
  }

  private def scheduleReloads() = {
    scheduledPool.scheduleAtFixedRate(new Runnable() { override def run() = reload() }, 5, 5, TimeUnit.SECONDS)
  }

  private def reload() = {
    try {
      snapshot = Await.result(getFreshData(), 5 seconds)
    } catch {
      case reason: Throwable ⇒ log.error("Error reloading snapshot", reason)
    }
  }

  def getFreshData()(implicit executor: ExecutionContext): Future[T]

}

