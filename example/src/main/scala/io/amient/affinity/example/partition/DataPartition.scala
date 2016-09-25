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
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.actor.{Partition, Region}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.serde.primitive.IntSerde
import io.amient.affinity.core.storage.MemStoreSimpleMap
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.example._

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

    new Node(config) {
      startRegion(new DataPartition)
    }
  }
}

class DataPartition extends Partition {

  // val config = context.system.settings.config

  //  val graph = storage {
  //    new NoopStorage[Int, VertexProps] with MemStoreSimpleMap[Int, VertexProps]
  //  }

  val graph = storage {
    new KafkaStorage[Int, VertexProps](brokers = "localhost:9092", topic = "graph", partition, classOf[IntSerde], classOf[MyAvroSerde])
      with MemStoreSimpleMap[Int, VertexProps]
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
      * getting VertexProps object by Vertex key
      */
    case vertex: Int => graph.get(vertex) match {
      case None => sender ! false
      case Some(vertexProps) => sender ! vertexProps
    }

    /**
      * Collect all connected vertices
      */
    case Component(vertex, group) => graph.get(vertex) match {
      case Some(existing) => sender ! Component(vertex, existing.edges.map(_.target))
      case None => sender ! Component(vertex, Set())
    }

    /**
      * Add an edge to the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the opeartion fails, true if the data was modified, false otherwise
      */
    case ModifyGraph(vertex, edge, GOP.ADD) => graph.get(vertex) match {

      case None => try {
        graph.put(vertex, Some(VertexProps(Set(edge))))
        sender ! true
      } catch {
        case NonFatal(e) => sender ! Status.Failure(e)
      }

      case Some(existing) if (existing.edges.exists(_.target == edge.target)) => sender ! false

      case Some(existing) => try {
        graph.put(vertex, Some(VertexProps(existing.edges + edge, existing.component)))
        sender ! true
      } catch {
        case NonFatal(e) => sender ! Status.Failure(e)
      }
    }

    /**
      * Remove an edge from the graph vertex.
      * This is a non-recursive operation, local to the
      * data shard owned by this partition.
      * Responds Status.Failure if the opeartion fails, true if the data was modified, false otherwise
      */
    case ModifyGraph(vertex, edge, GOP.REMOVE) => {
      graph.get(vertex) match {
        case None => sender ! false
        case Some(existing) => try {
          graph.put(vertex, Some(VertexProps(existing.edges.filter(_.target != edge.target), existing.component)))
          sender ! true
        } catch {
          case NonFatal(e) => sender ! Status.Failure(e)
        }
      }
    }

    /**
      * Updated component of the vertex.
      * Responds with the Component data previously associated with the vertex
      */
    case UpdateComponent(vertex, updatedComponent) => {
      graph.get(vertex) match {
        case None => sender ! Component(vertex, Set())
        case Some(existing) => try {
          graph.put(vertex, Some(VertexProps(existing.edges, updatedComponent)))
          sender ! Component(vertex, existing.component)
        } catch {
          case NonFatal(e) => sender ! Status.Failure(e)
        }
      }
    }

  }

}
