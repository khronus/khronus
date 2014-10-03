package com.despegar.metrik.model

case class StatisticSummary(p50: Double,p80: Double,p90: Double,p95: Double
							,p99: Double,p999: Double,min: Long,max:Long,count:Long,avg: Double)