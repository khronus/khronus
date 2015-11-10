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

package com.searchlight.khronus.store

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.searchlight.khronus.model.CounterSummary
import com.searchlight.khronus.util.Settings
import com.esotericsoftware.kryo.io.{ Input, Output }

import scala.concurrent.duration._

trait CounterSummaryStoreSupport extends SummaryStoreSupport[CounterSummary] {
  override def summaryStore: SummaryStore[CounterSummary] = Summaries.counterSummaryStore
}

class CassandraCounterSummaryStore(session: Session) extends CassandraSummaryStore[CounterSummary](session) {

  override def limit = Settings.Counter.SummaryLimit

  override def fetchSize = Settings.Counter.SummaryFetchSize

  override def tableName(duration: Duration) = s"counterSummary${duration.length}${duration.unit}"

  override def ttl(windowDuration: Duration): Int = Settings.Counter.SummaryRetentionPolicies(windowDuration).toSeconds.toInt

  override def serializeSummary(summary: CounterSummary): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    output.writeByte(1)
    output.writeVarLong(summary.count, true)
    output.flush()
    baos.flush()
    output.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  override def deserialize(timestamp: Long, buffer: Array[Byte]): CounterSummary = {
    val input = new Input(buffer)
    val version: Int = input.readByte
    if (version == 1) {
      //TODO: versioned
    }
    val count = input.readVarLong(true)
    input.close()
    CounterSummary(timestamp, count)
  }

}
