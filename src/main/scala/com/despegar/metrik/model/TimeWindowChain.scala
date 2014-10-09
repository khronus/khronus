package com.despegar.metrik.model

import scala.concurrent.duration._

class TimeWindowChain {

  val windows = Seq(TimeWindow(30 seconds, 1 millis), TimeWindow(1 minute, 30 seconds),
  	TimeWindow(5 minute, 1 minute, false))
  	
  def process(metric: String) = windows.foreach(_.process(metric))
  
}