package com.despegar.khronus.store

import com.despegar.khronus.util.BaseIntegrationTest
import org.scalatest.{Matchers, FunSuite}

import scala.concurrent.Await
import scala.concurrent.duration._

class LeaderElectionStoreTest extends FunSuite with BaseIntegrationTest with Matchers {

  override val tableNames: Seq[String] = Seq("leases")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  test("should acquire lock and then release it") {
    Await.result(LeaderElection.leaderElectionStore.acquireLock(), 2 seconds) shouldBe true

    Await.result(LeaderElection.leaderElectionStore.acquireLock(), 2 seconds) shouldBe false

    Await.result(LeaderElection.leaderElectionStore.releaseLock(), 2 seconds) shouldBe true
  }

  test("should renew the lock") {
    Await.result(LeaderElection.leaderElectionStore.acquireLock(), 2 seconds) shouldBe true
    Await.result(LeaderElection.leaderElectionStore.renewLock(), 2 seconds) shouldBe true
    Await.result(LeaderElection.leaderElectionStore.releaseLock(), 2 seconds) shouldBe true
  }

}
