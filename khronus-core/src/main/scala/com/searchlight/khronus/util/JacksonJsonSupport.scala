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

package com.searchlight.khronus.util

import com.fasterxml.jackson.databind.{ ObjectMapper, SerializationFeature }
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import spray.http.{ ContentTypes, HttpCharsets, HttpEntity, MediaTypes }
import spray.httpx.marshalling.Marshaller
import spray.httpx.unmarshalling.Unmarshaller

trait JacksonJsonSupport {
  lazy val jacksonModules = Seq(DefaultScalaModule, new AfterburnerModule())
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModules(jacksonModules: _*)
  mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)

  implicit def jacksonJsonUnmarshaller[T: Manifest] =
    Unmarshaller[T](MediaTypes.`application/json`) {
      case x: HttpEntity.NonEmpty ⇒
        val jsonSource = x.asString(defaultCharset = HttpCharsets.`UTF-8`)
        mapper.readValue[T](jsonSource)

    }

  implicit def jacksonJsonMarshaller[T <: AnyRef] =
    Marshaller.delegate[T, String](ContentTypes.`application/json`)(mapper.writeValueAsString(_))
}