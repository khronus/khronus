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

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.{ KryoSerializer, Logging }

import java.lang
import java.util.concurrent.Executors

import com.despegar.metrik.model.StatisticSummary
import com.despegar.metrik.util.{ Logging, KryoSerializer }
import com.netflix.astyanax.MutationBatch
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.{ ColumnFamily, ColumnList }
import com.netflix.astyanax.query.RowQuery
import com.netflix.astyanax.serializers.{ LongSerializer, StringSerializer }

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

case class ColumnRange(from: Long, to: Long, reversed: Boolean, count: Int)

trait StatisticSummarySupport extends SummaryStoreSupport[StatisticSummary] {
  override def summaryStore: SummaryStore[StatisticSummary] = CassandraStatisticSummaryStore
}

object CassandraStatisticSummaryStore extends SummaryStore[StatisticSummary] with Logging {
  //create column family definition for every bucket duration
  val windowDurations: Seq[Duration] = Seq(30 seconds, 1 minute, 5 minute, 10 minute, 30 minute, 1 hour)

  val serializer: KryoSerializer[StatisticSummary] = new KryoSerializer("statisticSummary", List(StatisticSummary.getClass))

  override def getColumnFamilyName(duration: Duration) = s"statisticSummary${duration.length}${duration.unit}"

  override def serializeSummary(summary: StatisticSummary): Array[Byte] = {
    serializer.serialize(summary)
  }

  override def deserialize(bytes: Array[Byte]): StatisticSummary = {
    serializer.deserialize(bytes)
  }


  def readAll(cf: Duration, key: String, from: Long, to: Long, count: Int): Future[Seq[StatisticSummary]] = Future {
    log.info(s"Reading cassandra: Cf: $cf - key: $key - From: $from - To: $to - Count: $count")
    val result = Vector.newBuilder[StatisticSummary]

    val query: RowQuery[String, lang.Long] = Cassandra.keyspace.prepareQuery(columnFamilies(cf))
      .getKey(key)
      .withColumnRange(from, to, false, count)
      .autoPaginate(true)

    readRecursive(result)(query.execute())
  }

  @tailrec
  private def readRecursive[A, B](resultBuilder: mutable.Builder[A, Vector[A]])(operation: ⇒ OperationResult[ColumnList[B]]): Seq[A] = {
    if (operation.getResult.isEmpty) resultBuilder.result().toSeq
    else {
      operation.getResult.asScala.foldLeft(resultBuilder) {
        (builder, column) ⇒
          builder += serializer.deserialize(column.getByteArrayValue).asInstanceOf[A]
      }
      readRecursive(resultBuilder)(operation)
    }

  }
}