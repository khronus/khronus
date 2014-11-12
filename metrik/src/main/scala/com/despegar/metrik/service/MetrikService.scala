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

package com.despegar.metrik.service

import akka.actor.Props
import akka.io.IO
import com.despegar.metrik.store.Cassandra
import com.despegar.metrik.util.{ MetrikStarted, Register, ActorSystemSupport, Settings }
import com.despegar.metrik.web.service.{ VersionActor, MetrikActor, MetrikHandler }
import spray.can.Http

trait MetrikService {
  this: ActorSystemSupport ⇒

  val handlerActor = system.actorOf(Props[MetrikHandler], "handler-actor")

  IO(Http) ! Http.Bind(handlerActor, Settings(system).Http.Interface, Settings(system).Http.Port)

  val metrikActor = system.actorOf(MetrikActor.props, MetrikActor.Name)
  val versionActor = system.actorOf(VersionActor.props, VersionActor.Name)

  handlerActor ! Register(MetrikActor.Path, metrikActor)
  handlerActor ! Register(VersionActor.Path, versionActor)

  Cassandra initialize

  system.eventStream.publish(MetrikStarted(handlerActor))
}