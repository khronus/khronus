package com.searchlight.khronus.api

case class ApiRequest(q: String, from: Option[String], to: Option[String], res: Option[String])
