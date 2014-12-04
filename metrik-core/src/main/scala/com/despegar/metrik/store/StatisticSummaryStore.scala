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

package com.despegar.metrik.store

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.Settings
import com.despegar.metrik.util.log.Logging
import com.esotericsoftware.kryo.io.{ Input, Output }

import scala.concurrent.duration._

case class ColumnRange(from: Long, to: Long, reversed: Boolean, count: Int)

trait StatisticSummarySupport extends SummaryStoreSupport[StatisticSummary] {
  override def summaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends SummaryStore[StatisticSummary] with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Settings().Histogram.WindowDurations

  override def getColumnFamilyName(duration: Duration) = s"statisticSummary${duration.length}${duration.unit}"

  override def serializeSummary(summary: StatisticSummary): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    output.writeByte(1) //version
    output.writeVarLong(summary.p50, true)
    output.writeVarLong(summary.p80, true)
    output.writeVarLong(summary.p90, true)
    output.writeVarLong(summary.p95, true)
    output.writeVarLong(summary.p99, true)
    output.writeVarLong(summary.p999, true)
    output.writeVarLong(summary.min, true)
    output.writeVarLong(summary.max, true)
    output.writeVarLong(summary.count, true)
    output.writeVarLong(summary.mean, true)
    output.flush()
    baos.flush()
    ByteBuffer.wrap(baos.toByteArray)
  }

  override def deserialize(timestamp: Long, buffer: ByteBuffer): StatisticSummary = {
    val input = new Input(buffer.array())
    val version = input.readByte()
    if (version == 1) {
      //TODO: versioned
    }
    val p50 = input.readVarLong(true)
    val p80 = input.readVarLong(true)
    val p90 = input.readVarLong(true)
    val p95 = input.readVarLong(true)
    val p99 = input.readVarLong(true)
    val p999 = input.readVarLong(true)
    val min = input.readVarLong(true)
    val max = input.readVarLong(true)
    val count = input.readVarLong(true)
    val mean = input.readVarLong(true)
    StatisticSummary(timestamp, p50, p80, p90, p95, p99, p999, min, max, count, mean)
  }

  override def ttl(windowDuration: Duration): Int = Settings().Histogram.SummaryRetentionPolicy

}

