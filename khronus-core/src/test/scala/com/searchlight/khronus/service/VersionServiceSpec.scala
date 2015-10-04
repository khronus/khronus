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

import com.searchlight.khronus.util.JacksonJsonSupport
import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._
import com.searchlight.khronus.model.Version
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

class VersionServiceSpec extends Specification with Specs2RouteTest with VersionEndpoint with JacksonJsonSupport {
  def actorRefFactory = ActorSystem("TestSystem", ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      |
      |  actor {
      |    provider = "akka.actor.LocalActorRefProvider"
      |  }
      | }
    """.stripMargin))
  override def createActorSystem(): ActorSystem = actorRefFactory

  "VersionService" should {

    "return version for GET requests to the version path" in {
      Get("/khronus/version") ~> versionRoute ~> check {
        val version = responseAs[Version]
        version.appName mustEqual "Khronus"
      }
    }

    "return a MethodNotAllowed error for PUT requests to the root path" in {
      Put("/khronus/version") ~> sealRoute(versionRoute) ~> check {
        status === MethodNotAllowed
        response.message.entity.asString === "HTTP method not allowed, supported methods: GET"
      }
    }
  }
}
