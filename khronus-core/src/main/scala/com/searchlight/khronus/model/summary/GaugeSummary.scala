package com.searchlight.khronus.model.summary

import com.searchlight.khronus.model.{ Summary, Timestamp }

case class GaugeSummary(timestamp: Timestamp, min: Long, max: Long, average: Long, count: Long) extends Summary
