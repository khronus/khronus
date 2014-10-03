package com.despegar.metrik

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.util.Timeout
import akka.pattern.ask
import spray.can.Http
import scala.concurrent.duration._
import com.despegar.metrik.web.service.HandlerActor
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import com.despegar.metrik.util.Logging
import com.despegar.metrik.store.Cassandra
import com.despegar.metrik.store.HistogramBucketStore
import com.despegar.metrik.store.CassandraHistogramBucketStore
import com.despegar.metrik.store.StatisticSummaryStore
import com.despegar.metrik.store.CassandraMetaStore
import com.despegar.metrik.store.CassandraStatisticSummaryStore

object Metrik extends App with Logging {

  log.info("Starting Metrik...")
  
  Cassandra initialize
  
  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("metrik-system")

  // create and start our service actor
  val handler = system.actorOf(Props[HandlerActor], "handler-actor")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(handler, interface = "localhost", port = 8080)
}


val system = ActorSystem("metrik-system")

