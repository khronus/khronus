package com.searchlight.khronus.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

case class Pool[T](name: String, instances: Int, createInstance: () ⇒ T, releaseInstance: (T) ⇒ Unit = { i: T ⇒ () }) {
  private val pooledInstancesCount = new AtomicLong(instances)
  private val pooledInstances = new ConcurrentLinkedQueue[T]()

  (1 to instances) foreach { _ ⇒ pooledInstances.offer(createInstance()) }

  def take(): T = {
    val pooledInstance = pooledInstances.poll()
    if (pooledInstance == null) {
      return createInstance()
    }
    pooledInstancesCount.decrementAndGet()
    return pooledInstance
  }

  def release(instance: T) = {
    releaseInstance(instance)
    if (pooledInstancesCount.incrementAndGet() <= instances) {
      pooledInstances.offer(instance)
    } else {
      pooledInstancesCount.decrementAndGet()
    }
  }

  def close() = {
    pooledInstances.clear()
  }
}

