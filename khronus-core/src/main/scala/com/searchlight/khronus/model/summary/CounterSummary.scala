package com.searchlight.khronus.model.summary

import com.searchlight.khronus.model.{ Summary, Timestamp }

case class CounterSummary(timestamp: Timestamp, count: Long) extends Summary
