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

import java.util.Properties

import io.amient.affinity.core.HttpInterface
import io.amient.affinity.core.actor.{Container, Region}
import io.amient.affinity.core.cluster.{Cluster, Coordinator, Node, ZkCoordinator}
import io.amient.affinity.example.rest.handler._

import scala.util.control.NonFatal

object ApiNode extends App {

  try {
    require(args.length >= 6)

    val actorSystemName = args(0)
    val akkaPort = args(1).toInt
    val host = args(2)
    val httpPort = args(3).toInt
    val numPartitions = args(4).toInt
    val partitionList = args(5) // coma-separated list of partitions assigned to this node
    val zkConnect = if (args.length > 6) args(6) else "localhost:2181"

    val affinityConfig = new Properties()
    affinityConfig.put(HttpInterface.CONFIG_HTTP_HOST, host)
    affinityConfig.put(HttpInterface.CONFIG_HTTP_PORT, httpPort.toString)
    affinityConfig.put(Cluster.CONFIG_NUM_PARTITIONS, numPartitions.toString)
    affinityConfig.put(Container.CONFIG_AKKA_SYSTEM, actorSystemName)
    affinityConfig.put(Container.CONFIG_AKKA_HOST, host)
    affinityConfig.put(Container.CONFIG_AKKA_PORT, akkaPort.toString)
    affinityConfig.put(Region.CONFIG_PARTITION_LIST, partitionList)
    affinityConfig.put(Coordinator.CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
    affinityConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT, zkConnect)

    //this API cluster is symmetric - all nodes serve both as Gateways and Regions
    new Node(affinityConfig) {

      startRegion(new ApiPartition(affinityConfig))

      startGateway(new HttpGateway(affinityConfig)
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

