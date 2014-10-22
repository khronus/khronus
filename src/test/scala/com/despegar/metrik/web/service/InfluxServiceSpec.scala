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
package com.despegar.metrik.web.service

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import com.despegar.metrik.web.service.influx.{ InfluxService, InfluxSeries }
import spray.http.StatusCodes._
import spray.http.Uri
import scala.concurrent.Future
import com.despegar.metrik.store.MetaStore
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito
import com.despegar.metrik.web.service.influx.InfluxSeriesProtocol._
import org.scalatest.BeforeAndAfter
import akka.actor.ActorRefFactory
import org.specs2.matcher.{ MatchResult, Expectable }
import spray.routing.HttpService

class InfluxServiceSpec extends Specification with Specs2RouteTest with MetrikExceptionHandler with MockitoSugar with HttpService {
  override val actorRefFactory = system

  class MockedInfluxService extends InfluxService {
    override val actorRefFactory = system

    override val metaStore: MetaStore = mock[MetaStore]
  }

  def applying[T](f: () ⇒ MatchResult[_]) = f()

  "InfluxService with error requests" should {
    "leave GET requests to other paths unhandled" in {
      applying {
        () ⇒
          Get("/kermit") ~> new MockedInfluxService().influxRoute ~> check {
            handled must beFalse
          }
      }
    }

    "return a MethodNotAllowed error for PUT requests to the path" in {
      applying {
        () ⇒
          {
            val uri = Uri("/metrik/influx").withQuery("q" -> "Some query")
            Put(uri) ~> sealRoute(new MockedInfluxService().influxRoute) ~> check {
              status === MethodNotAllowed
              responseAs[String] === "HTTP method not allowed, supported methods: GET"
            }

          }
      }
    }

    "return BadRequest for GET with an unsupported influx query" in {
      applying {
        () ⇒
          {
            val uri = Uri("/metrik/influx").withQuery("q" -> "Unsupported query")
            Get(uri) ~> sealRoute(new MockedInfluxService().influxRoute) ~> check {
              status === BadRequest
            }
          }
      }
    }

  }

  "InfluxService for listing series" should {
    val uri = Uri("/metrik/influx").withQuery("q" -> "list series")

    "return empty list when there isnt any metric" in {
      applying {
        () ⇒
          {
            val instance = new MockedInfluxService()

            Mockito.when(instance.metaStore.retrieveMetrics).thenReturn(Future(Seq()))

            Get(uri) ~> instance.influxRoute ~> check {

              handled must beTrue
              response.status == OK

              Mockito.verify(instance.metaStore).retrieveMetrics

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
            val instance = new MockedInfluxService()

            val firstMetric = "metric1"
            val secondMetric = "metric2"
            Mockito.when(instance.metaStore.retrieveMetrics).thenReturn(Future(Seq(firstMetric, secondMetric)))

            Get(uri) ~> instance.influxRoute ~> check {
              handled must beTrue
              status == OK

              Mockito.verify(instance.metaStore).retrieveMetrics

              val results = responseAs[Seq[InfluxSeries]]
              results.size must beEqualTo(2)

              results(0).name === firstMetric
              results(0).columns must beEmpty
              results(0).points must beEmpty

              results(1).name === secondMetric
              results(1).columns must beEmpty
              results(1).points must beEmpty
            }
          }
      }
    }

  }

}
