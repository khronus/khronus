package com.despegar.metrik.web.service

import com.despegar.metrik.model.Version
import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._
import com.despegar.metrik.model.MyJsonProtocol._
import com.despegar.metrik.model.Version
import spray.httpx.SprayJsonSupport._

class VersionServiceSpec extends Specification with Specs2RouteTest with VersionService {
  def actorRefFactory = system
  
  "VersionService" should {

    "return version for GET requests to the version path" in {
      Get("/metrik/version") ~> versionRoute ~> check {
        val version = responseAs[Version]
        version.nombreApp mustEqual("Metrik")
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
