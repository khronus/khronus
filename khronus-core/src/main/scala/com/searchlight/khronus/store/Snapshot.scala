package com.searchlight.khronus.store

import java.util.concurrent.TimeUnit

import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{ConcurrencySupport, Measurable}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait Snapshot[T] extends Logging with ConcurrencySupport with Measurable {

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
      snapshot = Await.result(getFreshData(), Duration.Inf)
    } catch {
      case reason: Throwable ⇒ log.error("Error reloading snapshot", reason)
    }
  }

  def getFreshData()(implicit executor: ExecutionContext): Future[T]

}

