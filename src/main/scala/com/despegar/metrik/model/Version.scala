package com.despegar.metrik.model

import spray.json.DefaultJsonProtocol

case class Version(nombreApp: String, version: String)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val VersionFormat = jsonFormat2(Version)
}