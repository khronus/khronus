package com.despegar.metrik.model

import java.lang.reflect.Method

import scala.collection.concurrent.TrieMap

trait Summary {
  def timestamp: Timestamp

  private val cache = TrieMap.empty[String, Method]

  def get(name: String): Long = {
    val method = cache.getOrElseUpdate(name, this.getClass.getDeclaredMethod(name))
    method.invoke(this).asInstanceOf[Long]
  }
}
