package com.despegar.khronus.influx.service

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._
import com.despegar.khronus.model.MyJsonProtocol._
import com.despegar.khronus.model.{ MetricType, Metric, Version }
import spray.httpx.SprayJsonSupport._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.despegar.khronus.service.VersionEndpoint
import org.scalatest.mock.MockitoSugar
import spray.routing.HttpService
import com.despegar.khronus.store.MetaStore
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
    val listSeriesURI = Uri(influxSeriesURI).withQuery("q" -> "list series /counter/", "u" -> "aUser", "p" -> "****")

    "return all existent metrics as influx series" in {
      applying {
        () ⇒
          {
            val instance = new MockedInfluxEndpoint()

            val counter = Metric("counter1", MetricType.Counter)
            val timer = Metric("timer1", MetricType.Timer)
            val searchExpression: String = ".*counter.*"

            Mockito.when(instance.metaStore.searchInSnapshot(searchExpression)).thenReturn(Future(Seq(counter)))

            Get(listSeriesURI) ~> instance.influxServiceRoute ~> check {
              handled must beTrue
              status == OK

              Mockito.verify(instance.metaStore).searchInSnapshot(searchExpression)

              import InfluxSeriesProtocol._
              val results = responseAs[Seq[InfluxSeries]]
              results.size must beEqualTo(1)

              results(0).name === counter.name
              results(0).columns must beEmpty
              results(0).points must beEmpty
            }
          }
      }
    }

  }

}
