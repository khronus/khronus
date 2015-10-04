package com.searchlight.khronus.model

import java.text.SimpleDateFormat
import java.util.Date

import com.searchlight.khronus.util.Settings
import com.searchlight.khronus.util.log.Logging
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

import scala.concurrent.duration._

class TickTest extends FunSuite with MockitoSugar with Matchers with Logging {

  val timeT = "2015-06-21T00:10:00"
  val timeTClock: Clock = new TestClock(timeT)

  test("delayed Tick at time T") {
    implicit val testClock: Clock = new TestClock("2015-06-21T00:10:00")

    val tick = Tick()

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:30"
  }

  test("delayed Tick at time T + 1s") {
    implicit val testClock: Clock = new TestClock("2015-06-21T00:10:01")

    val tick = Tick()

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:30"
  }

  test("delayed Tick at time T - 1s") {
    implicit val testClock: Clock = new TestClock("2015-06-21T00:09:59")

    val tick = Tick()

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:08:30"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
  }

  test("should not reprocess >= T-30s late samples arriving at time T+27s") {
    implicit val testClock: Clock = new TestClock(timeT) {
      override def now = super.now + (27 seconds).toMillis
    }
    Tick.alreadyProcessed(BucketNumber(timeTClock.now + (-30 seconds).toMillis, Settings.Window.RawDuration)) shouldBe false
  }

  test("should reprocess <= T-31s late sample arriving at time T") {
    implicit val testClock: Clock = timeTClock
    val sampleTime: Long = timeTClock.now - (31 seconds).toMillis
    Tick.alreadyProcessed(BucketNumber(sampleTime, Settings.Window.RawDuration)) shouldBe true
  }

  test("should reprocess <= T-1s late sample arriving at times T+28s") {
    implicit val testClock: Clock = new TestClock("2015-06-21T00:10:28")
    val sampleTime = timeTClock.now + (-1 seconds).toMillis
    Tick.alreadyProcessed(BucketNumber(sampleTime, Settings.Window.RawDuration)) shouldBe true
  }

  private def asStringDate(ts: Long)(implicit clock: Clock) = clock.asInstanceOf[TestClock].df.format(new Date(ts))
}

class TestClock(date: String) extends Clock {
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def now: Long = df.parse(date).getTime
}

object TestClock {
  def apply(date: String) = new TestClock(date)
}