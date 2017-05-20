package com.searchlight.khronus.model.query

import com.searchlight.khronus.model.{ Bucket, BucketNumber, LazyBucket }

import scala.collection.SortedMap
import scala.concurrent.Future

case class Group(selector: Selector, dimensions: Map[String, String], buckets: Future[SortedMap[BucketNumber, LazyBucket[Bucket]]]) {
  def label: String = if (dimensions.isEmpty) selector.label else s"${selector.label}$dimensionsLabel"

  private def dimensionsLabel = dimensions.keySet.toSeq.sorted.map { key ⇒ s"$key=${dimensions(key)}" }.mkString("[", ",", "]")
}
