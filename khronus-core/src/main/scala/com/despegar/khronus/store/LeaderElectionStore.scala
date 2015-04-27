package com.despegar.khronus.store

import com.datastax.driver.core.{ ResultSet, Session }
import com.despegar.khronus.util.ConcurrencySupport

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure

class LeaderElectionStore(session: Session) extends CassandraUtils with ConcurrencySupport {

  implicit val asyncExecutionContext = executionContext("leaderElection-store-worker")

  createSchema()

  private def createSchema() = {
    session.execute("CREATE TABLE IF NOT EXISTS leases (name text PRIMARY KEY, owner uuid) with default_time_to_live = 30")
  }

  private val uuid = java.util.UUID.randomUUID.toString
  private val name = "master"

  private val insert = session.prepare(s"INSERT INTO leases (name, owner) VALUES ('$name',$uuid) IF NOT EXISTS;")

  private val update = session.prepare(s"UPDATE leases set owner = $uuid where name = '$name' IF owner = $uuid;")

  private val delete = session.prepare(s"DELETE FROM leases where name = '$name' IF owner = $uuid;")

  def acquireLock(): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(insert.bind())

    f.map { resultSet ⇒
      resultSet.asScala.headOption.map(row ⇒ row.getBool("[applied]")).getOrElse(false)
    }
  }

  def renewLock(): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(update.bind())
    f.map { resultSet ⇒
      resultSet.asScala.headOption.map(row ⇒ row.getBool("[applied]")).getOrElse(false)
    }
  }

  def releaseLock(): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(delete.bind())
    f.map { resultSet ⇒
      resultSet.asScala.headOption.map(row ⇒ row.getBool("[applied]")).getOrElse(false)
    }
  }

}
