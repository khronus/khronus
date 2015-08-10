package com.searchlight.khronus.store

import com.searchlight.khronus.model._
import com.searchlight.khronus.util.Settings
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }
import scala.concurrent.duration._

class BucketCacheTest extends FunSuite with MockitoSugar with Matchers {

  test("two consecutive Ticks maintain affinity") {
    val cache: InMemoryCounterBucketCache.type = buildCache

    val tick09_00a09_30 = Tick()(new TestClock("2015-06-21T00:10:00"))
    val fromTick1 = tick09_00a09_30.bucketNumber
    val toTick1 = tick09_00a09_30.bucketNumber.following

    val metric = new Metric("tito", MetricType.Counter)
    cache.multiSet(metric, fromTick1, toTick1, Seq(new CounterBucket(fromTick1, 10l)))
    cache.markProcessedTick(tick09_00a09_30, metric)

    cache.nCachedMetrics(MetricType.Counter).intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().iterator.next() shouldBe fromTick1

    //change to the next tick
    val tick09_30a10_00 = Tick()(new TestClock("2015-06-21T00:10:30"))
    val fromTick2 = toTick1
    val toTick2 = tick09_30a10_00.bucketNumber.following

    cache.multiSet(metric, fromTick2, toTick2, Seq(new CounterBucket(fromTick2, 15l)))
    cache.markProcessedTick(tick09_30a10_00, metric)

    cache.nCachedMetrics(MetricType.Counter).intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().size shouldBe 2

    val slice = cache.multiGet(metric, 1 minute, fromTick1, toTick2)
    slice.isDefined shouldBe true
    slice.get.results.size shouldBe 2 //2 buckets of 30s

    cache.markProcessedTick(tick09_30a10_00, metric)
    cache.cachesByMetric.get(metric).get.keySet().size shouldBe 0 //in multiGet we remove values from cache
  }

  test("non consecutive ticks must lost affinity") {
    val cache: InMemoryCounterBucketCache.type = buildCache

    val tick09_00a09_30 = Tick()(new TestClock("2015-06-21T00:10:00"))
    val fromTick1 = tick09_00a09_30.bucketNumber
    val toTick1 = tick09_00a09_30.bucketNumber.following

    val metric = new Metric("tito", MetricType.Counter)
    cache.multiSet(metric, fromTick1, toTick1, Seq(new CounterBucket(fromTick1, 10l)))
    cache.markProcessedTick(tick09_00a09_30, metric)

    cache.nCachedMetrics(MetricType.Counter).intValue() shouldBe 1
    cache.cachesByMetric.get(metric).get.keySet().iterator.next() shouldBe fromTick1

    //change to the next non consecutive tick
    val tick10_00a10_30 = Tick()(new TestClock("2015-06-21T00:11:00"))
    val fromTick2 = tick10_00a10_30.bucketNumber
    val toTick2 = tick10_00a10_30.bucketNumber.following

    cache.multiSet(metric, fromTick2, toTick2, Seq(new CounterBucket(fromTick2, 15l)))
    cache.markProcessedTick(tick10_00a10_30, metric)

    cache.nCachedMetrics(MetricType.Counter).intValue() shouldBe 0
  }

  test("huge slice is ignored by cache") {
    val cache = buildCache

    //1 minute duration must slice (2 * MaxStore) buckets of 30 seconds
    var exceed = cache.sliceExceeded(1 minute, BucketNumber(47894183, 30 seconds), BucketNumber(47894186, 30 seconds))
    exceed shouldBe true

    exceed = cache.sliceExceeded(5 minute, BucketNumber(47894183, 1 minute), BucketNumber(47894190, 1 minute))
    exceed shouldBe true
  }

  test("Common slice should not exceed limit") {
    val cache = buildCache

    var exceed = cache.sliceExceeded(1 minute, BucketNumber(47894184, 30 seconds), BucketNumber(47894186, 30 seconds))
    exceed shouldBe false

    exceed = cache.sliceExceeded(5 minute, BucketNumber(47894184, 1 minute), BucketNumber(47894189, 1 minute))
    exceed shouldBe false
  }

  def buildCache: InMemoryCounterBucketCache.type = {
    val cache = InMemoryCounterBucketCache
    cache.cachesByMetric.clear()
    cache.nCachedMetrics(MetricType.Counter).set(0)
    cache.lastKnownTick.set(null)
    cache
  }
}
