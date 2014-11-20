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

import com.despegar.metrik.model.CounterSummary
import com.despegar.metrik.util.Settings
import com.esotericsoftware.kryo.io.{ UnsafeInput, UnsafeOutput }

import scala.concurrent.duration._

trait CounterSummaryStoreSupport extends SummaryStoreSupport[CounterSummary] {
  override def summaryStore: SummaryStore[CounterSummary] = CassandraCounterSummaryStore
}

object CassandraCounterSummaryStore extends SummaryStore[CounterSummary] {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Settings().Histogram.WindowDurations

  override def getColumnFamilyName(duration: Duration) = s"counterSummary${duration.length}${duration.unit}"

  override def serializeSummary(summary: CounterSummary): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val output = new UnsafeOutput(baos)
    output.writeByte(1)
    output.writeVarLong(summary.count, true)
    output.flush()
    baos.flush()
    output.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  override def deserialize(timestamp: Long, buffer: ByteBuffer): CounterSummary = {
    val input = new UnsafeInput(buffer.array())
    val version = input.read
    if (version == 1) {
      //TODO: versioned
    }
    val count = input.readVarLong(true)
    input.close()
    CounterSummary(timestamp, count)
  }

  override def ttl(windowDuration: Duration): Int = Settings().Counter.SummaryRetentionPolicy
}
