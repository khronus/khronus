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

import akka.actor.Props
import com.searchlight.khronus.model.Version
import com.searchlight.khronus.util.JacksonJsonSupport
import spray.http.MediaTypes._
import spray.routing._

class VersionActor extends HttpServiceActor with VersionEndpoint with KhronusHandlerException {
  def receive =
    runRoute(versionRoute)
}

object VersionActor {
  def props = Props[VersionActor]

  val Name = "version-actor"
  val Path = "khronus/version"
}

trait VersionEndpoint extends HttpService with JacksonJsonSupport {
  val versionRoute: Route =
    get {
      respondWithMediaType(`application/json`) {
        // XML is marshalled to `text/xml` by default, so we simply override here
        complete {
          Version("Khronus", "0.0.1-ALPHA")
        }
      }
    }
}
