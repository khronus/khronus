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

package com.searchlight.khronus

import com.searchlight.khronus.cluster.ClusterSupport
import com.searchlight.khronus.service.ActorSystemSupport
import com.searchlight.khronus.service.KhronusService
import com.searchlight.khronus.store.CassandraSupport
import com.searchlight.khronus.util.log.Logging

object Khronus extends App with Logging {
  log.info("""
               _   ___
              | | / / |
              | |/ /| |__  _ __ ___  _ __  _   _ ___
              |    \| '_ \| '__/ _ \| '_ \| | | / __|
              | |\  \ | | | | | (_) | | | | |_| \__ \
              \_| \_/_| |_|_|  \___/|_| |_|\__,_|___/""")
  KhronusApp
}

object KhronusApp extends ActorSystemSupport with CassandraSupport with KhronusService with ClusterSupport