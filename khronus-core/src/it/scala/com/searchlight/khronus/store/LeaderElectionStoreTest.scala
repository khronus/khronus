package com.searchlight.khronus.store

import java.util
import java.util.concurrent.{Callable, Executors}
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.driver.core.WriteType
import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.searchlight.khronus.util.log.Logging
import com.searchlight.khronus.util.{BaseIntegrationTest, Settings}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class LeaderElectionStoreTest extends FunSuite with BaseIntegrationTest with Matchers with Logging {

  override val tableNames: Seq[String] = Seq("leases")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  test("should acquire lock and then release it") {
    Await.result(LeaderElection.leaderElectionStore.acquireLock(), 2 seconds) shouldBe true

    Await.result(LeaderElectionOne.leaderElectionStore.acquireLock(), 2 seconds) shouldBe false
    Await.result(LeaderElectionTwo.leaderElectionStore.acquireLock(), 2 seconds) shouldBe false

    Await.result(LeaderElection.leaderElectionStore.releaseLock(), 2 seconds) shouldBe true
  }

  test("should renew the lock") {
    Await.result(LeaderElection.leaderElectionStore.acquireLock(), 2 seconds) shouldBe true
    Await.result(LeaderElection.leaderElectionStore.renewLock(), 2 seconds) shouldBe true
    Await.result(LeaderElection.leaderElectionStore.releaseLock(), 2 seconds) shouldBe true
  }

  test("should hold the lock") {
    val stores = List(LeaderElectionOne.leaderElectionStore, LeaderElectionTwo.leaderElectionStore, LeaderElectionThree.leaderElectionStore)
    //one obtain the lock
    val f = LeaderElectionOne.leaderElectionStore.acquireLock() map { x =>
      x shouldBe true
      stores foreach (store => {
        val lock = store.acquireLock();
        val hasLock = Await.result(lock, 2 seconds)
        hasLock shouldBe false
      })
    }

    Await.result(f, 5 seconds)

    Await.result(LeaderElectionOne.leaderElectionStore.releaseLock(), 2 seconds) shouldBe true
  }
}

object LeaderElectionOne extends CassandraKeyspace {
  initialize()

  val leaderElectionStore = new LeaderElectionStore(session)

  override def keyspace = "leaderElection"

  override def getRF: Int = Settings.CassandraLeaderElection.ReplicationFactor
}

object LeaderElectionTwo extends CassandraKeyspace {
  initialize()

  val leaderElectionStore = new LeaderElectionStore(session)

  override def keyspace = "leaderElection"

  override def getRF: Int = Settings.CassandraLeaderElection.ReplicationFactor
}

object LeaderElectionThree extends CassandraKeyspace {
  initialize()

  val leaderElectionStore = new LeaderElectionStore(session)

  override def keyspace = "leaderElection"

  override def getRF: Int = Settings.CassandraLeaderElection.ReplicationFactor
}


//object LightweightTransactions extends App with Matchers {
//
//  import scala.concurrent.ExecutionContext.Implicits.global
//
//  doIt()
//
//  def doIt() = {
//    val pool = Executors.newFixedThreadPool(10)
//    val stores = List(LeaderElectionOne.leaderElectionStore, LeaderElectionTwo.leaderElectionStore, LeaderElectionThree.leaderElectionStore)
//    val locksAdquire = new AtomicInteger()
//    val tasks = scala.collection.mutable.MutableList[Callable[Unit]]()
//    for (i <- 0 to 2) {
//      tasks += new Callable[Unit] {
//        override def call(): Unit = {
//          val start = System.currentTimeMillis()
//          val store = stores(i)
//          while ((System.currentTimeMillis() - start) < (30 seconds).toMillis) {
//            var hasLock = false
//              try {
//                hasLock = Await.result(store.acquireLock(), 1 second)
//                if (hasLock) println(s"${System.currentTimeMillis()} HAS THE LOCK: ${store.uuid}")
//              } catch {
//                case ex: Throwable => ex.printStackTrace()
//              }
//
//            try {
//              if (hasLock) {
//                locksAdquire.incrementAndGet()
//                Thread.sleep(100l)
//                val release = Await.result(store.releaseLock(), 1 second)
//                if (release) {
//                    locksAdquire.decrementAndGet()
//                    println(s"${System.currentTimeMillis()} release the lock: ${store.uuid}")
//                    hasLock = false
//                } else {
//                    println(s"${System.currentTimeMillis()} COULD NOT RELEASE THE LOCK: ${store.uuid}, hasLock: $hasLock, release: $release")
//                    sys.exit(-1)
//                }
//              }
//
//              Thread.sleep(100l)
//            } catch {
//              case ex: Throwable => {
//                ex.printStackTrace();
//                try {
//                  //retry release
//                  store.releaseLock() map (release => if (release) {
//                    locksAdquire.decrementAndGet();
//                    println("release the lock AFTER fail in release the lock. This should not happend")
//                  })
//                } catch {
//                  case ex: Throwable => println("Fail to try to release the lock after fail in adquire the lock! Unknow state!")
//                }
//              }
//            }
//          }
//        }
//      }
//    }
//
//    import scala.collection.JavaConversions._
//    pool.invokeAll(tasks.toList)
//    pool.shutdownNow()
//
//    println(s"FINISH. locks counts: ${locksAdquire.get()}")
//
//    sys.exit()
//  }
//}
