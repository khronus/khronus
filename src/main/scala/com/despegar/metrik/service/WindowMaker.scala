package com.despegar.metrik.service

import scala.concurrent.duration._
import com.despegar.metrik.model.Window

class WindowMaker {

  val windows = Seq(new Window(30 seconds, 0 seconds), new Window(1 minute, 30 seconds), new Window(5 minute, 1 minute, false))
  
  def process(metric: String) = {
    windows.foreach{ window => 
      window.process(metric) 
    }
  }
  
}