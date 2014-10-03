package com.despegar.metrik.service

import scala.concurrent.duration._
import com.despegar.metrik.model.TimeWindow

class TimeWindowChain {

  val windows = Seq(new TimeWindow(30 seconds, 0 seconds), new TimeWindow(1 minute, 30 seconds), 
  	new TimeWindow(5 minute, 1 minute, false))
  
  def process(metric: String) = windows.foreach(_.process(metric))
  
}