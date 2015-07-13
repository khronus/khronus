package com.despegar.khronus.store

import java.util.concurrent.atomic.AtomicLong

import com.despegar.khronus.model._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

class BucketCacheTest extends FunSuite with MockitoSugar with Matchers {

  test("two consecutive Ticks maintain affinity") {
    val cache = InMemoryCounterBucketCache
    cache.cachesByMetric.clear()
    cache.nCachedMetrics.set(0)
    cache.globalLastKnownTick.set(null)

    val tick09_00a09_30 = Tick()(new TestClock("2015-06-21T00:10:00"))
    val fromTick1 = tick09_00a09_30.bucketNumber
    val toTick1 = tick09_00a09_30.bucketNumber.following

    val metric = new Metric("tito", MetricType.Counter)
    cache.multiSet(metric, fromTick1, toTick1, Seq(new CounterBucket(fromTick1, 10l)))
    cache.markProcessedTick(tick09_00a09_30)

    cache.nCachedMetrics.intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().iterator.next() shouldBe fromTick1

    //change to the next tick
    val tick09_30a10_00 = Tick()(new TestClock("2015-06-21T00:10:30"))
    val fromTick2 = toTick1
    val toTick2 = tick09_30a10_00.bucketNumber.following

    cache.multiSet(metric, fromTick2, toTick2, Seq(new CounterBucket(fromTick2, 15l)))
    cache.markProcessedTick(tick09_30a10_00)

    cache.nCachedMetrics.intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().size shouldBe 2

    val slice = cache.multiGet(metric, fromTick1, toTick2)
    slice.isDefined shouldBe true
    slice.get.results.size shouldBe 2 //2 buckets of 30s

    cache.markProcessedTick(tick09_30a10_00)
    cache.cachesByMetric.get(metric).get.keySet().size shouldBe 0 //in multiGet we remove values from cache
  }

  test("non consecutive ticks must lost affinity") {
    val cache = InMemoryCounterBucketCache
    cache.cachesByMetric.clear()
    cache.nCachedMetrics.set(0)
    cache.globalLastKnownTick.set(null)

    val tick09_00a09_30 = Tick()(new TestClock("2015-06-21T00:10:00"))
    val fromTick1 = tick09_00a09_30.bucketNumber
    val toTick1 = tick09_00a09_30.bucketNumber.following

    val metric = new Metric("tito", MetricType.Counter)
    cache.multiSet(metric, fromTick1, toTick1, Seq(new CounterBucket(fromTick1, 10l)))
    cache.markProcessedTick(tick09_00a09_30)

    cache.nCachedMetrics.intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().iterator.next() shouldBe fromTick1

    //change to the next non consecutive tick
    val tick10_00a10_30 = Tick()(new TestClock("2015-06-21T00:11:00"))
    val fromTick2 = tick10_00a10_30.bucketNumber
    val toTick2 = tick10_00a10_30.bucketNumber.following

    cache.multiSet(metric, fromTick2, toTick2, Seq(new CounterBucket(fromTick2, 15l)))
    cache.markProcessedTick(tick10_00a10_30)

    cache.nCachedMetrics.intValue() shouldBe 0
  }
}
