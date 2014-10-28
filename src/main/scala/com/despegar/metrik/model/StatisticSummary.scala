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

case class StatisticSummary(timestamp: Long, p50: Double, p80: Double, p90: Double, p95: Double, p99: Double, p999: Double, min: Long, max: Long, count: Long, avg: Double) extends Summary {

  override def getTimestamp = timestamp
}

object StatisticSummary {
  implicit def reflector(ref: StatisticSummary) = new {
    def get(name: String): Long = ref.getClass.getMethods.find(_.getName == name).get.invoke(ref).asInstanceOf[Long]
  }
}