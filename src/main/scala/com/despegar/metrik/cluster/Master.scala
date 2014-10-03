package com.despegar.metrik.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.FromConfig

class Master extends Actor with ActorLogging {

  import com.despegar.metrik.cluster.Master._

  val masterInfo = {
    val config = context.system.settings.config.getConfig("metrik.master")
    val cronExpression = config.getString("cron-expression")

    MasterConfig(cronExpression)
  }

  override def preStart(): Unit = {
    super.preStart()
    initialize()
  }

  def initialize():Unit = {
    import us.theatr.akka.quartz._

    val scheduler = context.actorOf(Props[QuartzActor])
    scheduler ! AddCronSchedule(self, masterInfo.cronExpression, Tick)

    val router = context.actorOf(Props[Worker].withRouter(FromConfig()), "workerRouter")

    self ! Initialize(masterInfo.cronExpression, router)
  }

  def receive: Receive = {
    case Initialize(cronExpression, router) =>
      log.info("Master initialized with cron-expression: [{}]", cronExpression)

      context become doWork(router)
  }

  def doWork(router:ActorRef): Receive = {
    case Tick => router ! MasterWorkerProtocol.Work("myserie")
    case MasterWorkerProtocol.Finish(series) => println("Finished sserie " + series)
  }
}

object Master {
  case class Tick()
  case class Initialize(cronExpression:String, router:ActorRef)

  case class MasterConfig(cronExpression:String)

  def props: Props = Props(classOf[Master])
}

object MasterWorkerProtocol {
  case class Finish(series:String)
  case class Work(series:String)
}