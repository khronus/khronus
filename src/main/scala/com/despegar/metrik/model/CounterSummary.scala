package com.despegar.metrik.model

case class CounterSummary(timestamp: Long, count: Long) extends Summary {

  override def getTimestamp: Long = timestamp
}
