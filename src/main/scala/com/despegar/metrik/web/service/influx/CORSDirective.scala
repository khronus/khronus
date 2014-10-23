package com.despegar.metrik.web.service.influx

import spray.http.{ HttpMethods, HttpMethod, HttpResponse, AllOrigins }
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.routing._


trait CORSSupport {
  this: HttpService ⇒

  private val allowOriginHeader = `Access-Control-Allow-Origin`(AllOrigins)
  private val optionsCorsHeaders = List(
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(1728000))

  def cors[T]: Directive0 = mapRequestContext {
    ctx ⇒
      ctx.withRouteResponseHandling({
        //It is an option request for a resource that responds to some other method
        case Rejected(x) if (ctx.request.method.equals(HttpMethods.OPTIONS) && !x.filter(_.isInstanceOf[MethodRejection]).isEmpty) ⇒ {
          val allowedMethods: List[HttpMethod] = x.filter(_.isInstanceOf[MethodRejection]).map(rejection ⇒ {
            rejection.asInstanceOf[MethodRejection].supported
          })
          ctx.complete(HttpResponse().withHeaders(
            `Access-Control-Allow-Methods`(OPTIONS, allowedMethods: _*) :: allowOriginHeader ::
              optionsCorsHeaders))
        }
      }).withHttpResponseHeadersMapped {
        headers ⇒
          allowOriginHeader :: headers

      }
  }
}