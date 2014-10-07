package com.despegar.metrik.cluster

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._
import akka.routing.{Broadcast, FromConfig}
import us.theatr.akka.quartz.{AddCronScheduleFailure, _}

class Master extends Actor with ActorLogging {

  import Master._
  import context._

  var idleWorkers = Set[ActorRef]()
  var pendingMetrics = Seq[String]()

  val settings = Settings(system).Master

  override val supervisorStrategy =  OneForOneStrategy() {
    case _: ActorInitializationException ⇒ Stop
    case _: Exception                => Restart
  }

  self ! Initialize

  def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case Initialize =>
      val router = actorOf(Props[Worker].withRouter(FromConfig()), "workerRouter")
      val tickScheduler = actorOf(Props[QuartzActor])

      system.scheduler.schedule(settings.DiscoveryStartDelay, settings.DiscoveryInterval, router, Broadcast(DiscoverWorkers))
      tickScheduler ! AddCronSchedule(self, settings.TickCronExpression, Tick, true)

      become(initialized(router))

    case AddCronScheduleFailure(reason) => throw reason
    case everythingElse => //ignore
  }

  def getMetrics(): Seq[String] = Seq("a","b","c", "d","e")

  def initialized(router: ActorRef): Receive = {
    case Tick =>
      pendingMetrics ++= getMetrics().filterNot(metric => pendingMetrics contains metric)

      while (pendingMetrics.nonEmpty && idleWorkers.nonEmpty) {
        val worker = idleWorkers.head
        val pending = pendingMetrics.head

        worker ! Work(pending)

        idleWorkers = idleWorkers.tail
        pendingMetrics = pendingMetrics.tail
      }

    case Register(worker) =>
      log.info("Registring worker [{}]", worker.path)
      watch(worker)
      idleWorkers += worker

    case WorkDone(worker) =>
      if (pendingMetrics.nonEmpty) {
        worker ! Work(pendingMetrics.head)
        pendingMetrics = pendingMetrics.tail
      } else {
        idleWorkers += worker
      }

    case Terminated(worker) =>
      log.info("Removing worker [{}] from worker list", worker.path)
      idleWorkers -= worker
  }
}

object Master {
  case object Tick
  case class Initialize(cronExpression: String, router: ActorRef)
  case class MasterConfig(cronExpression: String)

  def props: Props = Props(classOf[Master])
}
