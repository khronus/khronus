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
import spray.http._
import StatusCodes._
import com.despegar.metrik.model.MyJsonProtocol._
import com.despegar.metrik.model.Version
import spray.httpx.SprayJsonSupport._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.despegar.metrik.service.VersionEndpoint

class VersionServiceSpec extends Specification with Specs2RouteTest with VersionEndpoint {
  def actorRefFactory = ActorSystem("TestSystem", ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  loglevel = INFO
      |  stdout-loglevel = DEBUG
      | }
    """.stripMargin))
  override def createActorSystem(): ActorSystem = actorRefFactory

  "VersionService" should {

    "return version for GET requests to the version path" in {
      Get("/metrik/version") ~> versionRoute ~> check {
        val version = responseAs[Version]
        version.nombreApp mustEqual ("Metrik")
      }
    }

    "leave GET requests to other paths unhandled" in {
      Get("/kermit") ~> versionRoute ~> check {
        handled must beFalse
      }
    }

    "return a MethodNotAllowed error for PUT requests to the root path" in {
      Put("/metrik/version") ~> sealRoute(versionRoute) ~> check {
        status === MethodNotAllowed
        responseAs[String] === "HTTP method not allowed, supported methods: GET"
      }
    }
  }
}
