package com.despegar.metrik.stress

import java.util.concurrent.Executors

import com.despegar.metrik.model.{ MetricMeasurement, Measurement, MetricBatch }

import scala.concurrent.Await
import scala.util.Random

object StressTest extends App {

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
            postToMetrikApi(i, j)
          }
        })
      }

      Thread.sleep(sleep)
      tasks foreach (task ⇒ task.get())
    } while (j < n)

    def postToMetrikApi(call: Int, run: Int): Unit = {
      val metricMeasurements = (for (i ← 1 to nMetrics) yield MetricMeasurement(s"metric$i", "timer", getMeasurements())) toList

      println(s"calling Metrik #$call, run #$run ${MetricBatch(metricMeasurements)}")
    }

    def getMeasurements(): List[Measurement] = {
      List(Measurement(System.currentTimeMillis(), getMeasurementValues()))
    }

    def getMeasurementValues(): Seq[Long] = {
      for (i ← 1 to nnMeasurements) yield (random.nextInt(10000)).toLong
    }
  }
}
