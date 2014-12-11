package com.despegar.khronus.influx.service

import akka.actor.ActorSystem
import com.despegar.khronus.model.{ Metric, MetricType }
import com.despegar.khronus.store.MetaStore
import com.despegar.khronus.util.JacksonJsonSupport
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import spray.http.StatusCodes._
import spray.http._
import spray.routing.HttpService
import spray.testkit.Specs2RouteTest

import scala.concurrent.Future

class InfluxServiceSpec extends Specification with MockitoSugar with HttpService with Specs2RouteTest with JacksonJsonSupport {
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
              println(response.message)
              response.message.entity.asString === "HTTP method not allowed, supported methods: GET, OPTIONS"
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
