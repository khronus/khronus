package com.despegar.metrik.web.service

import akka.actor.{ Props, Actor }
import akka.io.IO
import com.despegar.metrik.model.MyJsonProtocol._
import com.despegar.metrik.model.Version
import spray.can.Http
import spray.http.MediaTypes._
import spray.httpx.SprayJsonSupport._
import spray.routing._

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class VersionActor extends Actor with VersionService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(versionRoute)
}

// this trait defines our service behavior independently from the service actor
trait VersionService extends HttpService {

  val versionRoute =
    path("metrik" / "version") {
      get {
        respondWithMediaType(`application/json`) {
          // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            Version("Metrik", "0.0.1-ALPHA")
          }
        }
      }
    }
}
