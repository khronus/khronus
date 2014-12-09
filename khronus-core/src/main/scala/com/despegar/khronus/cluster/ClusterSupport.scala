/*
 * =========================================================================================
 * Copyright © 2014 the khronus project <https://github.com/hotels-tech/khronus>
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

package com.despegar.khronus.cluster

import akka.actor.PoisonPill
import akka.contrib.pattern.ClusterSingletonManager
import com.despegar.khronus.service.ActorSystemSupport

trait ClusterSupport {
  this: ActorSystemSupport ⇒

  system.actorOf(ClusterDomainEventListener.props, "cluster-listener")
  system.actorOf(ClusterSingletonManager.props(Master.props, "master", PoisonPill, Some("master")), "singleton-manager")
}
