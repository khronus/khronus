package com.despegar.metrik.model

import java.util.UUID

import com.netflix.astyanax.annotations.Component
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer
import com.netflix.astyanax.util.TimeUUIDUtils

class UniqueTimestamp {
  @Component(ordinal = 0)
  var measurementTimestamp: Long = _
  @Component(ordinal = 1)
  var timeUUID: UUID = _
}

object UniqueTimestamp {
  val serializer = new AnnotatedCompositeSerializer[UniqueTimestamp](classOf[UniqueTimestamp])
  def apply(timestamp: Timestamp): UniqueTimestamp = {
    val uniqueTimestamp = new UniqueTimestamp()
    uniqueTimestamp.measurementTimestamp = timestamp.ms
    uniqueTimestamp.timeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis
    uniqueTimestamp
  }
}