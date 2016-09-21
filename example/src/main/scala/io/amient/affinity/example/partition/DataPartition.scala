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

package io.amient.affinity.example.partition

import akka.actor.Status
import io.amient.affinity.example.{Component, MyAvroSerde, Vertex}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Partition, Region}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.{KafkaStorage, MemStoreSimpleMap}

import scala.collection.JavaConverters._
import scala.collection.immutable.Set
import scala.util.control.NonFatal


object DataPartition {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Service Node requires 2 argument: <akka-port> <node-partition-list>")

    val akkaPort = args(0).toInt
    val partitionList = args(1).split("\\,").map(_.toInt).toList.asJava

    val config = ConfigFactory.load("example")
      .withValue(Node.CONFIG_AKKA_PORT, ConfigValueFactory.fromAnyRef(akkaPort))
      .withValue(Region.CONFIG_PARTITION_LIST, ConfigValueFactory.fromIterable(partitionList))

    //this API cluster is symmetric - all nodes serve both as Gateways and Regions
    new Node(config) {

      startRegion(new DataPartition)

    }
  }
}

class DataPartition extends Partition {

  //val config = context.system.settings.config

  val graph = state {
    new KafkaStorage[Vertex, Component](brokers = "localhost:9092", topic = "graph", partition, classOf[MyAvroSerde], classOf[MyAvroSerde])
      with MemStoreSimpleMap[Vertex, Component]
  }

  override def handle: Receive = {

    /**
      * Simulating Partition Failure - the default supervision should restart this actor
      * after exeception is thrown
      */
    case (p: Int, stateError: IllegalStateException) =>
      require(p == partition)
      throw stateError

    /**
      * Describe partition and its stats
      */
    case (p: Int, "down") => context.system.terminate()

    /**
      * Describe partition and its stats
      */
    case (p: Int, "describe") =>
      require(p == partition)
      sender ! Map(
        "partition" -> partition,
        "graph" -> Map(
          "size" -> graph.size
        ))

    /**
      * Viewing a Component by Vertex key
      */
    case vertex: Vertex => graph.get(vertex) match {
      case None => sender ! false
      case Some(component) => sender ! component
    }

    /**
      * Adding edges to a component identified by vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      */
    case component@Component(vertex, newEdges, op) =>
      graph.get(vertex) match {
        case Some(existing) => connect(existing, newEdges)
        case None => connect(Component(vertex, Set[Vertex]()), newEdges)
      }
  }

  private def connect(component: Component, withVertices: Set[Vertex]): Unit = {
    try {
      withVertices.diff(component.edges).toList match {
        case Nil => sender ! false
        case additionalEdges =>
          val updatedComponent = Component(component.key, component.edges ++ additionalEdges)
          graph.put(component.key, Some(updatedComponent))
          sender ! true
      }
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
        sender ! Status.Failure(e)
    }
  }


}
