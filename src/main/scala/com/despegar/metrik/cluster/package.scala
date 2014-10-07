package com.despegar.metrik

import akka.actor.ActorRef

package object cluster {
    case class Register(worker: ActorRef)
    case class Work(metrics: String)
    case class WorkDone(worker: ActorRef)
    case object DiscoverWorkers
}