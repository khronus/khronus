package com.despegar.khronus.cluster

import akka.actor.{Actor, Props}
import com.despegar.khronus.store.LeaderElection
import com.despegar.khronus.util.ConcurrencySupport

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class LeaderElectionActor extends Actor with ConcurrencySupport {
  import context.dispatcher

  val tick =
    context.system.scheduler.schedule(500 millis, 1000 millis, self, "tick")

  override def postStop() = tick.cancel()

  val ex = executionContext("leaderElectionActor-worker", 50)

  def receive = {
    case "tick" => {
      LeaderElection.leaderElectionStore.acquireLock().onComplete{
        case Success(acquire) => {
          if (acquire) {
            try {
              log.info("<-------- LEADER ELECTION RESULT: iam the leader!")
              Thread.sleep(5000l)
            } finally {
              LeaderElection.leaderElectionStore.releaseLock()
              log.info("<-------- LEADER ELECTION: release the lock!")
            }
          } else {
            log.info("<-------- LEADER ELECTION RESULT: NOT the leader!")
          }
        }
        case Failure(ex) => log.error("Error in leader election",ex)
      }(ex)
    }
  }
}

object LeaderElectionActor {
  def props: Props = Props(classOf[LeaderElectionActor])
}
