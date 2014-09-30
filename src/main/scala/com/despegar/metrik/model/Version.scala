package com.despegar.metrik.model

import spray.json.DefaultJsonProtocol

/**
 * Created by dberjman on 9/30/14.
 */
case class Version(nombreApp: String, version: String)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val VersionFormat = jsonFormat2(Version)
}