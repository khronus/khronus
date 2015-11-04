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

package com.searchlight.khronus.stress

import java.util.concurrent.Executors
import akka.actor.{ Props, ActorSystem }
import com.searchlight.khronus.util.JacksonJsonSupport
import com.typesafe.config.ConfigFactory
import spray.http._
import spray.client.pipelining._

import com.searchlight.khronus.model._

import scala.concurrent.Future
import scala.util.{ Success, Failure, Random }
import spray.http._
import spray.client.pipelining._

import spray.http.HttpRequest
import com.searchlight.khronus.model.Measurement
import com.searchlight.khronus.model.MetricMeasurement
import scala.util.Failure
import spray.http.HttpResponse
import scala.util.Success
import com.searchlight.khronus.model.MetricBatch

object StressTest extends App with JacksonJsonSupport {
  implicit val system = ActorSystem("StressActorSystem")

  import system.dispatcher // execution context for futures

  doIt()

  def showHowToDoIt() = {
    println("StressTest concurrent times sleep nMetrics nMeasurements")
    System.exit(0)
  }

  def doIt(): Unit = {
    if (args.length < 1) {
      showHowToDoIt()
    }

    val c = args(0).toInt
    val n = args(1).toInt
    val sleep = args(2).toLong
    val nMetrics = args(3).toInt
    val nnMeasurements = args(4).toInt

    val random = new Random()

    val executor = Executors.newFixedThreadPool(c)

    var j = 0
    do {
      j += 1
      val tasks = for (i ← 1 to c) yield {
        executor.submit(new Runnable {
          override def run(): Unit = {
            postToKhronusApi(i, j)
          }
        })
      }

      Thread.sleep(sleep)
      tasks foreach (task ⇒ task.get())
    } while (j < n)

    def postToKhronusApi(call: Int, run: Int): Unit = {
      val metricMeasurements = (for (i ← 1 to nMetrics) yield MetricMeasurement(s"cachorra$i", MetricType.Timer, getMeasurements())) toList

      val metricBatch: MetricBatch = MetricBatch(metricMeasurements)
      println(s"calling Khronus #$call, run #$run")

      val request = Post("http://khronus-beta-00:9290/khronus/metrics", metricBatch)
      val pipeline: HttpRequest ⇒ Future[HttpResponse] = sendReceive

      val response = pipeline(request)
      response onComplete {
        case Failure(ex)   ⇒ ex.printStackTrace()
        case Success(resp) ⇒ println("success: " + resp.status)
      }
    }

    def getMeasurements(): List[Measurement] = {
      List(Measurement(Some(System.currentTimeMillis()), getMeasurementValues()))
    }

    def getMeasurementValues(): Seq[Long] = {
      for (i ← 1 to nnMeasurements) yield (random.nextInt(10000)).toLong
    }

    println("Ending...")
    executor.shutdown()
    system.terminate()
  }
}
