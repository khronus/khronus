/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */
package com.searchlight.khronus.util.log

import com.searchlight.khronus.model.{ Metric, Timestamp }
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.time.FastDateFormat
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

trait Logging {

  import com.searchlight.khronus.util.log.Logging._

  def loggerName = this.getClass().getName()

  val log = Logger(LoggerFactory.getLogger(loggerName))

  def p(metric: Metric, duration: Duration): String = p(metric, duration.toString)

  def p(metric: Metric, duration: String): String = s"[$metric-$duration]"

  def p(metric: Metric, ts: Long): String = p(metric, df.format(ts))

  def date(ts: Long): String = df.format(ts)

  def date(ts: Timestamp): String = date(ts.ms)

}

object Logging {
  val df = FastDateFormat.getInstance("HH:mm:ss")
}