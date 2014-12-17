Khronus - A reactive time series database [![Build Status](https://api.travis-ci.org/hotels-tech/khronus.png)](https://travis-ci.org/hotels-tech/khronus)
======

## Overview

Khronus is a reactive time series database. It is designed to store, analyze and process a huge amount of metrics.
It measures well. It correctly and precisely analyze and process timers and gauges using the great HdrHistogram by Gil Tene.
It is space efficient and has data tunable retention policies. It relies on both Akka cluster and Cassandra to scale and being resilient.
It is very fast to query percentiles, counts, min, max and others from metrics even if they have a lot of measurements.
Khronus does not have its own dashboard to graph it's metrics. It is focused on analyzing and retrieving time series data.
Currently it can be integrated with Grafana through the InfluxDB api.

## Status

Khronus is being actively developed. It is currently being used in production at Despegar.com
We plan to integrate it with Kibana too.


## Features

* It supports timers, gauges and counters.
* ...


## Installation

### Download Khronus

### Install a Cassandra cluster

### Configure

### Run


## Implementation details

  * Built in Scala
  * Akka cluster
  * Spray.io
  * HdrHistogram
  * Cassandra


## Contributions

Khronus is open to the community to collaborations and contributions

