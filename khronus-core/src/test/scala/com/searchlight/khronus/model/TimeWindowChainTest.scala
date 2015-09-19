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

package com.searchlight.khronus.model

import com.searchlight.khronus.store.MetaStore
import org.scalatest.{ BeforeAndAfter, FunSuite }
import org.scalatest.mock.MockitoSugar
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

class TimeWindowChainTest extends FunSuite with MockitoSugar with BeforeAndAfter {

  val metric1 = Metric("someMetric1", MetricType.Timer)
  val metric2 = Metric("someMetric2", MetricType.Timer)

  var window30s: HistogramTimeWindow = _
  var window1m: HistogramTimeWindow = _
  var mockedWindows: Seq[HistogramTimeWindow] = _
  var chain: TimeWindowChain = _

  before {
    window30s = mock[HistogramTimeWindow]
    window1m = mock[HistogramTimeWindow]
    mockedWindows = Seq(window30s, window1m)
    when(window30s.duration).thenReturn(30 seconds)
    when(window1m.duration).thenReturn(1 minute)
    when(window30s.process(any[Metric], any[Tick])).thenReturn(Future {})
    when(window1m.process(any[Metric], any[Tick])).thenReturn(Future {})

    chain = new TimeWindowChain {
      override val histogramsWindows = mockedWindows
      override val metaStore = mock[MetaStore]
      override val countersWindows = Seq.empty[CounterTimeWindow]
      override val windows = Map(MetricType.Counter -> countersWindows, MetricType.Timer -> histogramsWindows)
    }

    when(chain.metaStore.update(any[Seq[Metric]], any[Long], any[Boolean])).thenReturn(Future {})
  }

  test("should not execute TimeWindow of 1m") {
    implicit val clock = TestClock("2014-11-07T08:58:00")

    val tick = Tick()

    when(chain.metaStore.getLastProcessedTimestamp(metric1)).thenReturn(Future.successful((tick.bucketNumber - 1).endTimestamp()))

    val result = chain.process(Seq(metric1))

    Await.result(result, 5 seconds)

    verify(window30s).process(metric1, tick)
    verify(window1m, never()).process(metric1, tick)
  }

  test("should execute TimeWindow 1m previously failed") {
    implicit val clock = TestClock("2014-11-07T08:58:00")

    val tick = Tick()

    when(chain.metaStore.getLastProcessedTimestamp(metric1)).thenReturn(Future.successful((tick.bucketNumber - 2).endTimestamp()))

    val result = chain.process(Seq(metric1))

    Await.result(result, 5 seconds)

    verify(window30s).process(metric1, tick)
    verify(window1m).process(metric1, tick)
  }

  test("should tolerate metric failure") {
    implicit val clock = TestClock("2014-11-07T08:58:00")

    val tick = Tick()

    when(chain.metaStore.getLastProcessedTimestamp(metric1)).thenReturn(Future.successful((tick.bucketNumber - 1).endTimestamp()))
    when(chain.metaStore.getLastProcessedTimestamp(metric2)).thenReturn(Future.successful((tick.bucketNumber - 1).endTimestamp()))

    when(window30s.process(metric1, tick)).thenReturn(Future.failed(new RuntimeException()))
    when(window30s.process(metric2, tick)).thenReturn(Future.successful(()))

    val result = chain.process(Seq(metric1, metric2))

    Await.result(result, 5 seconds)

    verify(window30s).process(metric1, tick)
    verify(window30s).process(metric2, tick)
    verify(chain.metaStore).update(Seq(metric2), tick.endTimestamp)
  }
}
