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

import akka.actor.{ Props, Actor }
import akka.io.IO
import com.despegar.metrik.model.MyJsonProtocol._
import com.despegar.metrik.model.Version
import spray.can.Http
import spray.http.MediaTypes._
import spray.httpx.SprayJsonSupport._
import spray.routing._

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class VersionActor extends Actor with VersionService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(versionRoute)
}

// this trait defines our service behavior independently from the service actor
trait VersionService extends HttpService {

  val versionRoute =
    path("metrik" / "version") {
      get {
        respondWithMediaType(`application/json`) {
          // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            Version("Metrik", "0.0.1-ALPHA")
          }
        }
      }
    }
}
