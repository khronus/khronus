package com.searchlight.khronus.actor

import com.searchlight.khronus.util.log.Logging
import spray.routing._

import scala.util.Failure
import scala.util.control.NonFatal

object SprayMetrics extends Logging {

  import spray.routing.directives.BasicDirectives._

  def around(before: RequestContext ⇒ (RequestContext, Any ⇒ Any)): Directive0 =
    mapInnerRoute { inner ⇒
      ctx ⇒
        val (ctxForInnerRoute, after) = before(ctx)
        try inner(ctxForInnerRoute.withRouteResponseMapped(after))
        catch {
          case NonFatal(ex) ⇒ after(Failure(ex))
        }
    }

  def buildAfter(name: String, start: Long): Any ⇒ Any = { possibleRsp: Any ⇒
    possibleRsp match {
      case _ ⇒
        log.info(s"$name time spent ${System.currentTimeMillis() - start} ms")
    }
    possibleRsp
  }

  def time(name: String): Directive0 =
    around { ctx ⇒
      val timerContext = System.currentTimeMillis()
      (ctx, buildAfter(name, timerContext))
    }

}
