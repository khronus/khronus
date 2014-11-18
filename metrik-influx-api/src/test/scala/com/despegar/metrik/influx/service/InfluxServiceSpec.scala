package com.despegar.metrik.influx.service

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._
import com.despegar.metrik.model.MyJsonProtocol._
import com.despegar.metrik.model.{ MetricType, Metric, Version }
import spray.httpx.SprayJsonSupport._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.despegar.metrik.service.VersionEndpoint
import org.scalatest.mock.MockitoSugar
import spray.routing.HttpService
import com.despegar.metrik.store.MetaStore
import org.specs2.matcher.MatchResult
import org.mockito.Mockito
import scala.concurrent.Future
import InfluxSeriesProtocol._

class InfluxServiceSpec extends Specification with MockitoSugar with HttpService with Specs2RouteTest {
  def actorRefFactory = ActorSystem("TestSystem", ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      | }
    """.stripMargin))
  override def createActorSystem(): ActorSystem = actorRefFactory

  val influxSeriesURI = "/series"

  class MockedInfluxEndpoint extends InfluxEndpoint {
    override lazy val actorRefFactory = system

    override lazy val metaStore: MetaStore = mock[MetaStore]
  }

  def applying[T](f: () ⇒ MatchResult[_]) = f()

  "InfluxService with error requests" should {
    "leave GET requests to other paths unhandled" in {
      applying {
        () ⇒
          Get("/kermit") ~> new MockedInfluxEndpoint().influxServiceRoute ~> check {
            handled must beFalse
          }
      }
    }

    "return a MethodNotAllowed error for PUT requests to the path" in {
      applying {
        () ⇒
          {
            val uri = Uri(influxSeriesURI).withQuery("q" -> "Some query")
            Put(uri) ~> sealRoute(new MockedInfluxEndpoint().influxServiceRoute) ~> check {
              status === MethodNotAllowed
              responseAs[String] === "HTTP method not allowed, supported methods: GET, OPTIONS"
            }

          }
      }
    }

  }

  "InfluxService for listing series" should {
    val listSeriesURI = Uri(influxSeriesURI).withQuery("q" -> "list series", "u" -> "aUser", "p" -> "****")

    "return empty list when there isnt any metric" in {
      applying {
        () ⇒
          {
            val instance = new MockedInfluxEndpoint()

            Mockito.when(instance.metaStore.retrieveMetrics).thenReturn(Future(Seq()))

            Get(listSeriesURI) ~> instance.influxServiceRoute ~> check {

              handled must beTrue
              response.status == OK

              Mockito.verify(instance.metaStore).retrieveMetrics

              import InfluxSeriesProtocol._
              val results = responseAs[Seq[InfluxSeries]]
              results must beEmpty
            }
          }
      }
    }

    "return all existent metrics as influx series" in {
      applying {
        () ⇒
          {
            val instance = new MockedInfluxEndpoint()

            val firstMetric = Metric("metric1", MetricType.Timer)
            val secondMetric = Metric("metric2", MetricType.Timer)
            Mockito.when(instance.metaStore.retrieveMetrics).thenReturn(Future(Seq(firstMetric, secondMetric)))

            Get(listSeriesURI) ~> instance.influxServiceRoute ~> check {
              handled must beTrue
              status == OK

              Mockito.verify(instance.metaStore).retrieveMetrics

              import InfluxSeriesProtocol._
              val results = responseAs[Seq[InfluxSeries]]
              results.size must beEqualTo(2)

              results(0).name === firstMetric.name
              results(0).columns must beEmpty
              results(0).points must beEmpty

              results(1).name === secondMetric.name
              results(1).columns must beEmpty
              results(1).points must beEmpty
            }
          }
      }
    }

  }

}
