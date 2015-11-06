package com.searchlight.khronus.store

import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.datastax.driver.core.{ ConsistencyLevel, WriteType, ResultSet, Session }
import com.searchlight.khronus.util.ConcurrencySupport

import scala.concurrent.Future
import scala.collection.JavaConverters._
import scala.util.Success

/**
 * In a IF NOT EXISTS operation, if the record not exists it returns one column: [applied] = True. Otherwise
 * it returns the [applied] column in false plus the record (name, owner)
 *
 * @param session
 */
class LeaderElectionStore(session: Session) extends CassandraUtils with ConcurrencySupport {

  implicit val asyncExecutionContext = executionContext("leaderElection-store-worker")

  createSchema()

  private def createSchema() = {
    session.execute("CREATE TABLE IF NOT EXISTS leases (name text PRIMARY KEY, owner uuid) with default_time_to_live = 30")
  }

  val uuid = java.util.UUID.randomUUID.toString
  private val name = "master"

  private val insert = session.prepare(s"INSERT INTO leases (name, owner) VALUES ('$name',$uuid) IF NOT EXISTS;")

  private val update = session.prepare(s"UPDATE leases set owner = $uuid where name = '$name' IF owner = $uuid;")

  private val delete = session.prepare(s"DELETE FROM leases where name = '$name' IF owner = $uuid;")

  private val select = session.prepare(s"SELECT owner FROM leases where name = '$name';")

  def acquireLock(retry: Int = 0): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(insert.bind())

    f.recoverWith {
      case e: WriteTimeoutException ⇒ if (e.getWriteType.equals(WriteType.CAS)) {
        //paxos phase fails
        if (retry < 3) {
          log.error(s"Error in acquireLock($uuid). Fail CAS operation. Retrying...")
          acquireLock(retry + 1)
        } else {
          log.error(s"Fail to recover from acquireLock(retry = $retry)", e)
          throw e
        }
      } else {
        //commit phase fails. reading the row in serial will force Cassandra to commit any remaining uncommitted Paxos state before proceeding with the read
        renewLock()
      }
    }

    f.map(validateResult)
  }

  def validateResult(resultSet: ResultSet): Boolean = {
    resultSet.asScala.headOption.exists(row ⇒ {
      if (row.getBool("[applied]")) {
        true
      } else {
        row.getUUID("owner").toString.equals(uuid)
      }
    })
  }

  def renewLock(): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(update.bind())

    f.map(validateResult)
  }

  def softLockCheck(): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(select.bind().setConsistencyLevel(ConsistencyLevel.QUORUM)) //TODO serial read?

    f map {
      _.asScala.headOption.exists(row ⇒ {
        val hasLock = row.getUUID("owner").toString.equals(uuid)
        log.debug(s"softLockCheck for $uuid: $hasLock")
        hasLock
      })
    }
  }

  def releaseLock(retry: Int = 0): Future[Boolean] = {
    val f: Future[ResultSet] = session.executeAsync(delete.bind())

    f.recoverWith {
      case e: WriteTimeoutException ⇒ if (e.getWriteType.equals(WriteType.CAS)) {
        //paxos phase fails
        if (retry < 3) {
          log.error(s"Error in releaseLock($uuid). Fail CAS operation. Retrying...")
          releaseLock(retry + 1)
        } else {
          log.error(s"Fail to recover from releaseLock(retry = $retry)", e)
          throw e;
        }
      } else {
        //commit phase fails
        log.error(s"fail commit phase $uuid")
        softLockCheck()
      }
    }

    f.map { resultSet ⇒
      resultSet.asScala.headOption.map(row ⇒ row.getBool("[applied]")).getOrElse(false)
    } andThen {
      case Success(applied) ⇒ {
        log.debug(s"End releaseLock for $uuid and retries $retry -> $applied")
        if (!applied && retry != 0) softLockCheck() map (x ⇒ Future.successful(!x))
      }
    }
  }

}

