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

package com.searchlight.khronus.service

import akka.actor._
import akka.testkit._
import com.searchlight.khronus.service.HandShakeProtocol.Register
import com.typesafe.config.ConfigFactory
import org.scalatest._
import spray.http.HttpResponse
import spray.http.StatusCodes
import spray.httpx.RequestBuilding
import spray.routing.HttpServiceActor

class KhronusHandlerSpec extends TestKitBase with ImplicitSender with FunSpecLike with Matchers with RequestBuilding with BeforeAndAfterAll {

  implicit lazy val system: ActorSystem = ActorSystem("khronus-handler-spec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |  event-handlers = ["akka.event.Logging$DefaultLogger"]
      |  actor {
      |    provider = "akka.actor.LocalActorRefProvider"
      |  }
      |}
      |
      |khronus {
      |  master {
      |    tick-expression = "0/1 * * * * ?"
      |    discovery-start-delay = 1 second
      |    discovery-interval = 2 seconds
      |  }
      |  internal-metrics {
      |    enabled = false
      |  } 
      |}
    """.stripMargin))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it("returns 404 Not Found when nothing has been registered") {
    new Fixture {
      master ! Get("/endpoint")
      expectMsgType[HttpResponse].status shouldBe StatusCodes.NotFound
    }
  }

  it("can access an endpoint after registering it") {
    new Fixture {
      val endpointActor = TestActorRef(new DummyEndpoint)
      master ! Register("endpoint", endpointActor)
      master ! Get("/endpoint")
      val response = expectMsgType[HttpResponse]
      response.status shouldBe StatusCodes.OK
      response.entity.data.asString shouldBe "i'm alive"
    }
  }

  it("can register multiple endpoints") {
    new Fixture {
      val endpointActor1 = TestActorRef(new DummyEndpoint("1"))
      master ! Register("endpoint1", endpointActor1)
      val endpointActor2 = TestActorRef(new DummyEndpoint("2"))
      master ! Register("endpoint2", endpointActor2)

      Seq(
        "/endpoint1" → "1",
        "/endpoint2" → "2") foreach {
          case (endpoint, expected) ⇒
            master ! Get(endpoint)
            val response = expectMsgType[HttpResponse]
            response.status shouldBe StatusCodes.OK
            response.entity.data.asString shouldBe expected
        }
    }
  }

  trait Fixture {
    val master = TestActorRef(new KhronusHandler)
  }

}

class DummyEndpoint(response: String = "i'm alive") extends HttpServiceActor {
  def receive = runRoute {
    complete {
      response
    }
  }
}
