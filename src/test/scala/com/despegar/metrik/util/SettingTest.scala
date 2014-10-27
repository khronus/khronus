package com.despegar.metrik.util

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import com.despegar.metrik.model.CounterTimeWindow
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import com.despegar.metrik.model.HistogramTimeWindow

class SettingTest extends FunSuite with MockitoSugar with Matchers {

  implicit lazy val system: ActorSystem = ActorSystem("TestSystem", ConfigFactory.parseString(
    """
      |metrik {
      |  histogram {
      |    windows = [30 seconds, 1 minute]
      |  }
      |  counter {
      |    windows = [30 seconds]
      |  }
      |}
    """.stripMargin))

  test("should configure histogram time windows properly") {
    Settings(system).Histogram.timeWindows shouldBe (Seq(HistogramTimeWindow(30 seconds, 1 millis),
      HistogramTimeWindow(1 minute, 30 seconds, false)))
  }

  test("should configure counter time windows properly") {
    Settings(system).Counter.timeWindows shouldBe (Seq(CounterTimeWindow(30 seconds, 1 millis, false)))
  }

}