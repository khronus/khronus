/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.store

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.despegar.khronus.model.StatisticSummary
import com.despegar.khronus.util.{ Measurable, Settings }
import com.esotericsoftware.kryo.io.{ Input, Output }

import scala.concurrent.duration._
import com.despegar.khronus.util.log.Logging

trait StatisticSummarySupport extends SummaryStoreSupport[StatisticSummary] {
  override def summaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends CassandraSummaryStore[StatisticSummary] with Logging with Measurable {

  override def windowDurations: Seq[Duration] = Settings.Histogram.WindowDurations
  override def limit = Settings.Histogram.SummaryLimit
  override def fetchSize = Settings.Histogram.SummaryFetchSize

  override def tableName(duration: Duration) = s"statisticSummary${duration.length}${duration.unit}"
  override def ttl(windowDuration: Duration): Int = Settings.Histogram.SummaryRetentionPolicy

  override def serializeSummary(summary: StatisticSummary): ByteBuffer = {
    val byteArray = StatisticSummarySerializerV2.serialize(summary)
    recordGauge("statisticSummarySerializedBytes", byteArray.length)
    ByteBuffer.wrap(byteArray)
  }

  override def deserialize(timestamp: Long, buffer: Array[Byte]): StatisticSummary = {
    val input = new Input(buffer)
    val version = input.readByte()
    version match {
      case 1 ⇒ StatisticSummarySerializerV1.deserialize(input, timestamp)
      case 2 ⇒ StatisticSummarySerializerV2.deserialize(input, timestamp)
      case _ ⇒ throw new UnsupportedOperationException(s"Could not deserialize unknown statistic summary version $version")
    }
  }

}

trait StatisticSummarySerializer {
  def serialize(summary: StatisticSummary): Array[Byte]
  def deserialize(input: Input, timestamp: Long): StatisticSummary

}

object StatisticSummarySerializerV1 extends StatisticSummarySerializer {
  override def serialize(summary: StatisticSummary): Array[Byte] = {
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
    baos.toByteArray
  }

  override def deserialize(input: Input, timestamp: Long): StatisticSummary = {
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
}

object StatisticSummarySerializerV2 extends StatisticSummarySerializer {

  override def serialize(summary: StatisticSummary): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    output.writeByte(2) //version

    val min = summary.min
    val p50 = summary.p50 - min
    val p80 = summary.p80 - p50
    val p90 = summary.p90 - p80
    val p95 = summary.p95 - p90
    val p99 = summary.p99 - p95
    val p999 = summary.p999 - p99
    val max = summary.max - p999
    val mean = summary.mean - min

    output.writeVarLong(p50, true)
    output.writeVarLong(p80, true)
    output.writeVarLong(p90, true)
    output.writeVarLong(p95, true)
    output.writeVarLong(p99, true)
    output.writeVarLong(p999, true)
    output.writeVarLong(min, true)
    output.writeVarLong(max, true)
    output.writeVarLong(summary.count, true)
    output.writeVarLong(mean, true)
    output.flush()
    baos.flush()
    baos.toByteArray
  }

  override def deserialize(input: Input, timestamp: Long): StatisticSummary = {
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

    StatisticSummary(timestamp, min + p50, p50 + p80, p80 + p90, p90 + p95, p95 + p99, p99 + p999, min, p999 + max, count, min + mean)
  }
}

