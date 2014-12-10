package com.despegar.metrik.influx.service

case class Dashboard(name: String, columns: Vector[String] = Vector.empty, points: Vector[Vector[String]] = Vector.empty)
