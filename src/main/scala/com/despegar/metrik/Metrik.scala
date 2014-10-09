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

package com.despegar.metrik.util

import akka.actor._
import com.despegar.metrik.cluster.ClusterSupport
import com.despegar.metrik.web.service.MetrikService

trait ActorSystemSupport {
  implicit lazy val system = ActorSystem(Settings.Metrik.ActorSystem)
}

object Metrik extends App with ActorSystemSupport with MetrikService with ClusterSupport