package com.despegar.metrik.web.service

import akka.actor.Actor
import spray.routing.HttpService
import spray.http.MediaTypes._
import spray.httpx.SprayJsonSupport._
import spray.routing._
import spray.util.LoggingContext
import spray.http.StatusCodes._
import com.despegar.metrik.util.Logging

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class HandlerActor extends Actor with MetricsService with VersionService {

  implicit def myExceptionHandler =
    ExceptionHandler.apply {
      case e: UnsupportedOperationException => ctx => {
        ctx.complete(BadRequest)
      }
      case e: Exception => ctx => {
        log.error(e.getMessage(), e)
        ctx.complete(InternalServerError)
      }
    }

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  def receive = runRoute(metricsRoute ~ versionRoute)
}

