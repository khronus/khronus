package com.despegar.khronus.model

import java.text.SimpleDateFormat
import java.util.Date

import com.despegar.khronus.util.log.Logging
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class TickTest extends FunSuite with MockitoSugar with Matchers with Logging {
  test("after 30 seconds") {
    implicit val testClock = new TestClock("2015-06-21T00:10:00")

    val tick = Tick.current(testClock)

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:30"
  }

  test("after 31 seconds") {
    implicit val testClock = new TestClock("2015-06-21T00:10:01")

    val tick = Tick.current(testClock)

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:30"
  }

  test("after 29 seconds") {
    implicit val testClock = new TestClock("2015-06-21T00:09:59")

    val tick = Tick.current(testClock)

    log.info(s"$tick")
    tick.bucketNumber.duration shouldEqual (30 seconds)
    asStringDate(tick.bucketNumber.startTimestamp().ms) shouldEqual "2015-06-21T00:08:30"
    asStringDate(tick.bucketNumber.endTimestamp().ms) shouldEqual "2015-06-21T00:09:00"
  }


  private def asStringDate(ts: Long)(implicit clock: TestClock) = clock.df.format(new Date(ts))
}

class TestClock(date: String) extends Clock {
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def now: Long = df.parse(date).getTime
}