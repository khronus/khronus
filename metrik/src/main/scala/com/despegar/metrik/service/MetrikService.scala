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

import akka.io.IO
import com.despegar.metrik.service.HandShakeProtocol.{ MetrikStarted, Register }
import com.despegar.metrik.store.{ CassandraBuckets, CassandraMeta, CassandraSummaries }
import com.despegar.metrik.util.Settings
import spray.can.Http

trait MetrikService {
  this: ActorSystemSupport ⇒

  val handlerActor = system.actorOf(MetrikHandler.props, MetrikHandler.Name)

  IO(Http) ! Http.Bind(handlerActor, Settings.Http.Interface, Settings.Http.Port)

  val metrikActor = system.actorOf(MetrikActor.props, MetrikActor.Name)
  val versionActor = system.actorOf(VersionActor.props, VersionActor.Name)

  handlerActor ! Register(MetrikActor.Path, metrikActor)
  handlerActor ! Register(VersionActor.Path, versionActor)

  CassandraMeta.initialize
  CassandraBuckets.initialize
  CassandraSummaries.initialize

  system.eventStream.publish(MetrikStarted(handlerActor))
}