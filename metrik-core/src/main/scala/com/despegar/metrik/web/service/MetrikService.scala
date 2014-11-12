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

import akka.actor.{ ActorRefFactory, Props }
import akka.io.IO
import com.despegar.metrik.util.{ Settings, ActorSystemSupport }
import com.despegar.metrik.web.service.MetrikHandler.Register
import com.despegar.metrik.web.service.influx.{ InfluxEndpoint, InfluxActor }
import spray.can.Http

trait MetrikService {
  this: ActorSystemSupport ⇒

  val master = system.actorOf(Props[MetrikHandler], "metrik-handler")

  IO(Http) ! Http.Bind(master, Settings(system).Http.Interface, Settings(system).Http.Port)

  val metrikActor = system.actorOf(MetrikActor.props, "metrik-actor")
  master ! Register("metrik/metrics", metrikActor)

  val versionActor = system.actorOf(VersionActor.props, "version-actor")
  master ! Register("metrik/version", versionActor)

  val influxActor = system.actorOf(InfluxActor.props, "influx-actor")
  master ! Register("metrik/influx", influxActor)
}