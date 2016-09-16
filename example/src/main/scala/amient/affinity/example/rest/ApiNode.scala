/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.example.rest

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Gateway, Region}
import io.amient.affinity.core.cluster.{Cluster, Node}
import io.amient.affinity.example.rest.handler._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ApiNode extends App {

  try {
    require(args.length == 5, "Service Node requires 5 argument: <akka-port> <host> <http-port> <total-partitions> <node-partition-list>")

    val akkaPort = args(0).toInt
    val host = args(1)
    val httpPort = args(2).toInt
    val numPartitions = args(3).toInt
    val partitionList = args(4).split("\\,").map(_.toInt).toList.asJava

    val config = ConfigFactory.load("example")
      .withValue(Cluster.CONFIG_NUM_PARTITIONS, ConfigValueFactory.fromAnyRef(numPartitions))
      .withValue(Node.CONFIG_AKKA_HOST, ConfigValueFactory.fromAnyRef(host))
      .withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort))
      .withValue(Gateway.CONFIG_HTTP_HOST, ConfigValueFactory.fromAnyRef(host))
      .withValue(Gateway.CONFIG_HTTP_PORT, ConfigValueFactory.fromAnyRef(httpPort))
      .withValue(Region.CONFIG_PARTITION_LIST, ConfigValueFactory.fromIterable(partitionList))

    //this API cluster is symmetric - all nodes serve both as Gateways and Regions
    new Node(config) {

      startRegion(new ApiPartition(config))

      startGateway(new HttpGateway(config)
        with Describe
        with Ping
        with Fail
        with Connect
        with Access
      )

    }

  } catch {
    case e: IllegalArgumentException =>
      e.printStackTrace()
      System.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      System.exit(2)
    case e: Throwable =>
      e.printStackTrace()
      System.exit(3)
  }

}

