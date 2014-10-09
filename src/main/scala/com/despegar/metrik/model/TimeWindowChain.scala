package com.despegar.metrik.model

import scala.concurrent.duration._
import com.despegar.metrik.util.Logging

class TimeWindowChain extends Logging {

  val windows = Seq(TimeWindow(30 seconds, 1 millis), TimeWindow(1 minute, 30 seconds),
  	TimeWindow(5 minute, 1 minute, false))
  	
  def process(metric: String) = {
    log.debug(s"Processing windows for $metric...")
    windows.foreach(_.process(metric))
  }
  
}