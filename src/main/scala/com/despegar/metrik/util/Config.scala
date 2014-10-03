package com.despegar.metrik.util

import com.typesafe.config.ConfigFactory

trait Config {

  val config = ConfigFactory.load()
}
