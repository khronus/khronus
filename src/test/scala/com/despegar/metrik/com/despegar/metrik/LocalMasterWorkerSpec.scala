/*
 * =========================================================================================
 * Copyright © 2014 the metrik project <https://github.com/hotels-tech/metrik>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.despegar.metrik.com.despegar.metrik

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKitBase}
import com.despegar.metrik.cluster.Master
import com.despegar.metrik.cluster.Master.{Tick, Initialize}
import com.typesafe.config.ConfigFactory
import org.scalatest._

class LocalMasterWorkerSpec extends TestKitBase with WordSpecLike
                                                with Matchers
                                                with ImplicitSender {


  implicit lazy val system: ActorSystem = ActorSystem("metrik-spec", ConfigFactory.parseString(
  """
    |akka {
    |  loglevel = INFO
    |  stdout-loglevel = DEBUG
    |  event-handlers = ["akka.event.Logging$DefaultLogger"]
    |
    |  actor {
    |    provider = "akka.cluster.ClusterActorRefProvider"
    |
    |    deployment {
    |      /master/workerRouter {
    |        router = round-robin
    |        nr-of-instances = 4
    |        cluster {
    |          enabled = on
    |          max-nr-of-instances-per-node = 2
    |          allow-local-routees = on
    |        }
    |      }
    |    }
    |  }
    |
    |  remote {
    |    enabled-transports = ["akka.remote.netty.tcp"]
    |    log-remote-lifecycle-events = on
    |    netty.tcp {
    |      hostname = "127.0.0.1"
    |      port = 8080
    |    }
    |  }
    |
    |  cluster {
    |    seed-nodes = [
    |      "akka.tcp://metrik-system@127.0.0.1:2551",
    |      "akka.tcp://metrik-system@127.0.0.1:2552",
    |    ]
    |    roles = ["master"]
    |
    |    auto-down-unreachable-after = 10s
    |  }
    |}
    |
    |metrik {
    |  endpoint = "127.0.0.1"
    |  port = 8080
    |
    |  master {
    |    tick-expression = "0/1 * * * * ?"
    |    discovery-start-delay = 1 second
    |    discovery-interval = 5 seconds
    |  }
    |}
  """.stripMargin))

  system.actorOf(Master.props, "master")
  expectMsg(Tick)

  "The Metrik system" should {
    "" in {

    }
  }

}
