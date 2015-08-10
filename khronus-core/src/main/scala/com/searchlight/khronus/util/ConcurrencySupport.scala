package com.searchlight.khronus.util

import java.util.concurrent._

import com.searchlight.khronus.model.Monitoring
import com.searchlight.khronus.util.log.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

trait ConcurrencySupport extends Logging {

  private def fixedThreadPool(name: String, threads: Int): ExecutorService = {
    val pool = Executors.newFixedThreadPool(threads, ThreadFactory(name))
    pool.asInstanceOf[ThreadPoolExecutor].setRejectedExecutionHandler(new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
        if (!executor.isShutdown) {
          val exception = new RejectedExecutionException(s"Task $r rejected from pool $name = $executor")
          log.error(s"Reject task from $name", exception)
          throw exception
        }
      }
    })
    manage(pool, name)
    pool
  }

  def scheduledThreadPool(name: String, threads: Int = 1) = {
    val pool = Executors.newScheduledThreadPool(threads, ThreadFactory(name))
    manage(pool, name)
    pool
  }

  def executionContext(name: String, threads: Int = Runtime.getRuntime.availableProcessors()): ExecutionContext = {
    ExecutionContext.fromExecutor(fixedThreadPool(name, threads), (t: Throwable) ⇒ Unit)
  }

  private def manage(pool: ExecutorService, name: String) = if (Settings.InternalMetrics.Enabled) {
    ConcurrencySupport.register(name, pool.asInstanceOf[ThreadPoolExecutor])
    sys.addShutdownHook({
      log.info(s"Shutting down $name thread pool")
      pool.shutdown()
    })
  }

}

object ThreadFactory {
  def apply(name: String) = new ThreadFactoryBuilder().setNameFormat(s"$name-%d").build()
}

object ConcurrencySupport extends Measurable {

  private val pools: ConcurrentHashMap[String, ThreadPoolExecutor] = new ConcurrentHashMap[String, ThreadPoolExecutor]()
  private val monitoringScheduler = Executors.newScheduledThreadPool(1, ThreadFactory("pool-monitoring-worker"))
  sys.addShutdownHook({
    monitoringScheduler.shutdown()
  })

  def register(name: String, pool: ThreadPoolExecutor) = pools.put(name, pool)

  def startThreadPoolsMonitoring = {
    monitoringScheduler.scheduleAtFixedRate(new Runnable() {
      override def run() = reportPoolStatus()
    }, 0, 1, TimeUnit.SECONDS)
  }

  private def reportPoolStatus() = {
    pools.asScala.foreach { entry ⇒
      val name = entry._1
      val pool = entry._2
      pool.getPoolSize
      recordGauge(s"pool.$name.threads.total", pool.getPoolSize)
      recordGauge(s"pool.$name.threads.active", pool.getActiveCount)
      recordGauge(s"pool.$name.queue.size", pool.getQueue.size())
    }
  }

}

object SameThreadExecutionContext extends ExecutionContext with Logging {
  override def execute(runnable: Runnable): Unit = {
    runnable.run()
  }
  override def reportFailure(cause: Throwable): Unit = {}
}