package com.despegar.metrik.util

import java.util.concurrent._

import com.despegar.metrik.util.log.Logging
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

trait ConcurrencySupport extends Logging {

  import com.despegar.metrik.util.ConcurrencySupport._

  private def fixedThreadPool(name: String, threads: Int): ExecutorService = {
    val pool = Executors.newFixedThreadPool(threads, threadFactory(name))
    manage(pool, name)
    pool
  }

  def scheduledThreadPool(name: String, threads: Int = 1) = {
    val pool = Executors.newScheduledThreadPool(threads, threadFactory(name))
    manage(pool, name)
    pool
  }

  def executionContext(name: String, threads: Int = Runtime.getRuntime.availableProcessors()): ExecutionContext = {
    ExecutionContext.fromExecutor(fixedThreadPool(name, threads))
  }

  private def manage(pool: ExecutorService, name: String) = {
    ConcurrencySupport.register(name, pool.asInstanceOf[ThreadPoolExecutor])
    sys.addShutdownHook({
      log.info(s"Shutting down $name thread pool")
      pool.shutdown()
    })
  }

}

object ConcurrencySupport extends Measurable {

  private val pools: ConcurrentHashMap[String, ThreadPoolExecutor] = new ConcurrentHashMap[String, ThreadPoolExecutor]()
  private val monitoringScheduler = Executors.newScheduledThreadPool(1, threadFactory("pool-monitoring-worker"))
  sys.addShutdownHook({
    monitoringScheduler.shutdown()
  })

  private def threadFactory(name: String): ThreadFactory = {
    new ThreadFactoryBuilder().setNameFormat(s"$name-%d").build()
  }

  def register(name: String, pool: ThreadPoolExecutor) = pools.put(name, pool)

  monitoringScheduler.scheduleAtFixedRate(new Runnable() {
    override def run = reportPoolStatus
  }, 0, 2, TimeUnit.SECONDS)

  private def reportPoolStatus = {
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