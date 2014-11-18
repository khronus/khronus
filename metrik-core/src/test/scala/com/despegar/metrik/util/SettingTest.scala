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

package com.despegar.metrik.util

import akka.actor.ActorSystem
import com.despegar.metrik.model.{ CounterTimeWindow, HistogramTimeWindow }
import com.typesafe.config.ConfigFactory
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

import scala.concurrent.duration._

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
    Settings(system).Histogram.TimeWindows shouldBe (Seq(HistogramTimeWindow(30 seconds, 1 millis),
      HistogramTimeWindow(1 minute, 30 seconds, false)))
  }

  test("should configure counter time windows properly") {
    Settings(system).Counter.TimeWindows shouldBe (Seq(CounterTimeWindow(30 seconds, 1 millis, false)))
  }

}