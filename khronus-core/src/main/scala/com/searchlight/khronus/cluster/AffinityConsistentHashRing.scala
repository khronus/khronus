package com.searchlight.khronus.cluster

import akka.actor.ActorRef
import com.searchlight.khronus.model.Metric
import com.searchlight.khronus.util.Settings
import com.searchlight.khronus.util.log.Logging
import scala.util.hashing.MurmurHash3

class AffinityConsistentHashRing extends Logging {

  private val tokens = collection.mutable.SortedSet[Token]()(Ordering.by(_.hash))
  private val tokensByWorker = collection.mutable.Map[String, Seq[Token]]()
  private var metricsByWorker: Map[String, MetricsQueue] = Map[String, MetricsQueue]()

  private def virtualTokens(actor: String, count: Int = 256) = (1 to count).map(id ⇒ Token(hash(s"$actor-$id"), actor)).toSeq

  private def hash(string: String) = MurmurHash3.arrayHash(string.toArray)

  private def clockwiseToken(metric: Metric) = {
    tokens.from(Token(hash(metric.name))).headOption.getOrElse(tokens.head)
  }

  def addWorker(worker: ActorRef): Unit = {
    val workerKey = key(worker)
    if (!tokensByWorker.contains(workerKey)) {
      val workerTokens = virtualTokens(workerKey)
      tokensByWorker += ((workerKey, workerTokens))
      tokens ++= workerTokens
    }
  }

  def removeWorker(worker: ActorRef): Unit = {
    tokensByWorker.remove(key(worker)).foreach { workerTokens ⇒
      tokens --= workerTokens
    }
  }

  private def key(actor: ActorRef) = actor.path.parent.toString

  def assignWorkers(metrics: Seq[Metric]) = {
    if (tokens.nonEmpty) {
      metricsByWorker = metrics.groupBy(clockwiseToken(_).worker).map { case (workerKey, groupedMetrics) ⇒ (workerKey, MetricsQueue(groupedMetrics)) }
    }
  }

  def nextMetrics(worker: ActorRef): Seq[Metric] = metricsByWorker.get(key(worker)).map(_.next).getOrElse(Seq())
  def hasPendingMetrics(worker: ActorRef) = metricsByWorker.get(key(worker)).exists(_.hasNext)

  def remainingMetrics(): Seq[Metric] = {
    metricsByWorker.values flatMap (_.remaining) toSeq
  }

}

object AffinityConsistentHashRing {
  def apply() = new AffinityConsistentHashRing
}

case class Token(hash: Int, worker: String = "")

case class MetricsQueue(metrics: Seq[Metric]) {
  private val m = metrics.grouped(Settings.Master.WorkerBatchSize)
  def next = if (m.hasNext) m.next() else Seq()
  def hasNext = m.hasNext
  def remaining = m.toList.flatten
}