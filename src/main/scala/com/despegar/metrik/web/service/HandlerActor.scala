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

import akka.actor.Actor
import spray.routing._
import spray.http.StatusCodes._
import com.despegar.metrik.web.service.influx.InfluxService
import com.despegar.metrik.util.Logging

class HandlerActor extends Actor with MetrikExceptionHandler with MetricsService with VersionService with InfluxService {
  def actorRefFactory = context
  def receive = runRoute(metricsRoute ~ versionRoute ~ influxRoute)
}

trait MetrikExceptionHandler extends Logging {

  implicit def myExceptionHandler =
    ExceptionHandler.apply {
      case e: UnsupportedOperationException ⇒ ctx ⇒ {
        ctx.complete(BadRequest)
      }
      case e: Exception ⇒ ctx ⇒ {
        log.error(e.getMessage(), e)
        ctx.complete(InternalServerError)
      }
    }

}
