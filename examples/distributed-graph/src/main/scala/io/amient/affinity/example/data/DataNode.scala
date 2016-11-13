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

package io.amient.affinity.example.data

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.model.graph.GraphPartition

import scala.collection.JavaConverters._


object DataNode {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Service Node requires 2 argument: <akka-port> <node-partition-list>")

    val akkaPort = args(0).toInt
    val partitionList = args(1).split("\\,").map(_.toInt).toList.asJava

    val config = ConfigFactory.load("example")
      .withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort))
      .withValue(Node.CONFIG_PARTITION_LIST, ConfigValueFactory.fromIterable(partitionList))

    new Node(config) {
      startRegion(new Partition
        with GraphPartition
        with DebugPartition
      )
    }
  }
}
