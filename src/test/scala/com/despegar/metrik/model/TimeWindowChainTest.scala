package com.despegar.metrik.model

import com.despegar.metrik.store.MetaStore
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global

class TimeWindowChainTest  extends FunSuite with MockitoSugar {

  test("should execute TimeWindows that correspond to the tick time") {
    val window30s = mock[HistogramTimeWindow]
    val window1m = mock[HistogramTimeWindow]

    val mockedWindows: Seq[HistogramTimeWindow] = Seq(window30s, window1m)

    val chain = new TimeWindowChain {
      override val histrogramsWindows = mockedWindows
      override val metaStore = mock[MetaStore]
    }

    when(window30s.duration).thenReturn(30 seconds)
    when(window1m.duration).thenReturn(1 minute)

    when(chain.metaStore.update(any[Metric], any[Long])).thenReturn(Future{})

    val result = chain.process(Metric("tito", "timer"))

    Await.result(result, 5 seconds)
  }
}
