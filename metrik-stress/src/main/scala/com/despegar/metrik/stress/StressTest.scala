package com.despegar.metrik.stress

import java.util.concurrent.Executors
import akka.actor.{ Props, ActorSystem }
import com.typesafe.config.ConfigFactory
import spray.http._
import spray.client.pipelining._

import com.despegar.metrik.model.{ MetricBatchProtocol, Measurement, MetricBatch, MetricMeasurement }

import scala.concurrent.Future
import scala.util.{ Success, Failure, Random }
import spray.http._
import spray.client.pipelining._

import MetricBatchProtocol._

object StressTest extends App {
  implicit val system = ActorSystem("StressActorSystem", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |}
    """.stripMargin))

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
            postToMetrikApi(i, j)
          }
        })
      }

      Thread.sleep(sleep)
      tasks foreach (task ⇒ task.get())
    } while (j < n)

    def postToMetrikApi(call: Int, run: Int): Unit = {
      val metricMeasurements = (for (i ← 1 to nMetrics) yield MetricMeasurement(s"cachorra$i", "timer", getMeasurements())) toList

      val metricBatch: MetricBatch = MetricBatch(metricMeasurements)
      println(s"calling Metrik #$call, run #$run")

      val request = Post("http://ht-core-01:8080/metrik/metrics", metricBatch)
      val pipeline: HttpRequest ⇒ Future[HttpResponse] = sendReceive

      val response = pipeline(request)
      response onComplete {
        case Failure(ex)   ⇒ ex.printStackTrace()
        case Success(resp) ⇒ println("success: " + resp.status)
      }
    }

    def getMeasurements(): List[Measurement] = {
      List(Measurement(System.currentTimeMillis(), getMeasurementValues()))
    }

    def getMeasurementValues(): Seq[Long] = {
      for (i ← 1 to nnMeasurements) yield (random.nextInt(10000)).toLong
    }

    println("Ending...")
    executor.shutdown()
    system.shutdown()
  }
}
