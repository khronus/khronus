/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
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

package com.despegar.metrik.model

case class StatisticSummary(timestamp: Long, p50: Long, p80: Long, p90: Long, p95: Long, p99: Long, p999: Long, min: Long, max: Long, count: Long, avg: Double) extends Summary {

  import StatisticSummary._
  override def getTimestamp = timestamp

}

object StatisticSummary {
  implicit def reflector(ref: StatisticSummary) = new {
    def get(name: String): Long = ref.getClass.getMethods.find(_.getName == name).get.invoke(ref).asInstanceOf[Long]
  }

  def toMap(summary: StatisticSummary): Option[Map[String, AnyVal]] = {
    Some(Map(
      "min" -> summary.min,
      "max" -> summary.max,
      "p50"  -> summary.p50,
      "p80"  -> summary.p80,
      "p90"  -> summary.p90,
      "p95"  -> summary.p95,
      "p99"  -> summary.p99,
      "p999" -> summary.p999))
  }
}

