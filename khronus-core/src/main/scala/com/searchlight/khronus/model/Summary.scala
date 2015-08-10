package com.searchlight.khronus.model

import java.lang.reflect.Method

import scala.collection.concurrent.TrieMap

trait Summary {
  def timestamp: Timestamp

  def get(function: String) = Summary.get(function, this)
}

object Summary {

  private val cache = TrieMap.empty[String, Method]

  def get(name: String, summary: Summary): Long = {
    val klass = summary.getClass
    val method = cache.getOrElseUpdate(s"${klass.getName}|$name", klass.getDeclaredMethod(name))
    method.invoke(summary).asInstanceOf[Long]
  }
}
