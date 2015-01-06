package com.despegar.khronus.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

case class Pool[T](name: String, factoryFunction: () ⇒ T, initInstances: Int, releaseFunction: T ⇒ Unit = { i: T ⇒ () }) {
  val instances = new AtomicLong()
  instances.set(initInstances)
  val maxInstances = initInstances * 2
  val objects = new ConcurrentLinkedQueue[T]()

  (1 to initInstances) foreach { _ ⇒ objects.offer(factoryFunction()) }

  def take(): T = {
    val pooledInstance = objects.poll()
    if (pooledInstance == null) {
      return factoryFunction()
    }
    instances.decrementAndGet()
    return pooledInstance
  }

  def release(instance: T) = {
    releaseFunction(instance)
    if (instances.intValue() < maxInstances) {
      instances.incrementAndGet()
      objects.offer(instance)
    }
  }

  def close() = {
    objects.clear()
  }
}

