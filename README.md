Khronus - A reactive time series database [![Build Status](https://api.travis-ci.org/hotels-tech/khronus.png)](https://travis-ci.org/hotels-tech/khronus)
======

## Overview

Khronus is a open source distributed reactive time series database. It is designed to store, analyze and process a huge amount of metrics.

It measures well. It correctly and precisely analyze and process timers and gauges using the great HdrHistogram by Gil Tene.It is space efficient and has data tunable retention policies. It relies on both Akka cluster and Cassandra to scale and being resilient.
It is very fast to query percentiles, counts, min, max and others from metrics even if they have a lot of measurements.

Khronus does not have its own dashboard to graph it's metrics. It is focused on analyzing and retrieving time series data. Currently it can be integrated with Grafana through the InfluxDB api.

## Status

Khronus is being actively developed. It is currently being used in production at Despegar.com
We plan to integrate it with Kibana too.

## Features

* It supports timers, gauges and counters.
* Configurable series resolution (30 seconds, 1 minute, 10 minutes, etc)
* Measures well. No more average of percentiles.
* Fast, very fast retrieving of metrics.
* Scalable.
* High available.
* REST Api for push data
* Implements InfluxDB protocol for use with Grafana

## Installation

### Download Khronus

Go to releases and download the last stable version.

### Install a Cassandra cluster

Khronus requires Cassandra 2.x. For installation look [official documentation](https://wiki.apache.org/cassandra/GettingStarted)

### Configure

The main config file for overriding properties is located at ../conf/application.conf. Some useful configurations:

```json
khronus {
  # bind host
  endpoint = "127.0.0.1"
  port = 9290
  
  windows {
    # Delay the current time to avoid losing measures pushed after the current tick
    # It must be less than the minor window
    execution-delay = 20 seconds
  }

  internal-metrics {
    # all internal metrics has the preffix ~system
    enabled = true
  }

  histogram {
    # resolutions to be pre calculated
    windows = [30 seconds, 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour]
    # expiration ttl
    bucket-retention-policy = 6 hours
    # expiration ttl
    summary-retention-policy = 90 days
  }

  counter {
    # resolutions to be pre calculated
    windows = [30 seconds, 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour]
    # expiration ttl
    bucket-retention-policy = 6 hours
    # expiration ttl
    summary-retention-policy = 90 days
  }
  
  dashboards {
    # nroOfPoints = period / resolution
    # if the number of points is less than the minor, scale up in resolution
    min-resolution-points = 700
    # if the number of points is greater than the max, scale down in resolution
    max-resolution-points = 1500
  }

  master {
    # tick to process all the metrics
    tick-expression = "0/30 * * * * ?"
    # delay to send discovery (for new workers) message
    discovery-start-delay = 1 second
    discovery-interval = 5 seconds
  }

  cassandra {
    cluster {
      seeds = "127.0.0.1"
      port = 9042
      # useful is you are using an existing cluster
      keyspace-name-suffix = ""
    }

    meta {
      # replication factor
      rf = 3
    }

    buckets {
      # replication factor
      rf = 1
    }

    summaries {
      # replication factor
      rf = 1
    }
  }
}

```

### Run


## Implementation details

  * Built in Scala
  * Akka cluster
  * Spray.io
  * HdrHistogram
  * Cassandra


## Contributions

Khronus is open to the community to collaborations and contributions

