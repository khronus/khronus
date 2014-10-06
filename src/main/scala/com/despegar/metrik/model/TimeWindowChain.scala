package com.despegar.metrik.model

import scala.concurrent.duration._

class TimeWindowChain {

  val windows = Seq(new TimeWindow(30 seconds, 0 seconds), new TimeWindow(1 minute, 30 seconds), 
  	new TimeWindow(5 minute, 1 minute, false))
  
  def process(metric: String) = windows.foreach(_.process(metric))
  
}