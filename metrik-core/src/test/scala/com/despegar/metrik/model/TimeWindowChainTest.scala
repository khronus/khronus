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

import com.despegar.metrik.store.MetaStore
import com.despegar.metrik.util.Settings
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

class TimeWindowChainTest extends FunSuite with MockitoSugar {

  test("should execute TimeWindows that correspond to the tick time") {
    val window30s = mock[HistogramTimeWindow]
    val window1m = mock[HistogramTimeWindow]

    val mockedWindows: Seq[HistogramTimeWindow] = Seq(window30s, window1m)

    val chain = new TimeWindowChain {
      override val histrogramsWindows = mockedWindows
      override val metaStore = mock[MetaStore]
      override val countersWindows = Seq.empty[CounterTimeWindow]

      override def currentTick(windows: Seq[TimeWindow[_, _]]) = {
        Tick(BucketNumber(47178956, 30 seconds)) //this tick corresponds to the interval from 07/11/2014 08:58:00 to 07/11/2014 08:58:30
      }
    }

    when(window30s.duration).thenReturn(30 seconds)
    when(window1m.duration).thenReturn(1 minute)
    when(window30s.process(any[Metric], any[Tick])).thenReturn(Future {})
    when(window1m.process(any[Metric], any[Tick])).thenReturn(Future {})

    when(chain.metaStore.update(any[Metric], any[Long])).thenReturn(Future {})

    val metric = Metric("tito", MetricType.Timer)
    val result = chain.process(metric)

    Await.result(result, 5 seconds)

    verify(window30s).process(any[Metric], any[Tick])
    verify(window1m, never()).process(any[Metric], any[Tick])
  }
}
