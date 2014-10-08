package com.despegar.metrik.util

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Logging {

  def loggerName = this.getClass().getName()
  val log = Logger(LoggerFactory.getLogger(loggerName))

}